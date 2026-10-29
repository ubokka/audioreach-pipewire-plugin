/* Copyright (c) Qualcomm Technologies, Inc. and/or its subsidiaries.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <dirent.h>
#include <linux/input.h>
#include <spa/utils/result.h>
#include <spa/utils/string.h>
#include <spa/utils/json.h>
#include <spa/utils/ringbuffer.h>
#include <spa/debug/types.h>
#include <spa/pod/builder.h>
#include <spa/pod/parser.h>
#include <spa/node/node.h>
#include <spa/node/utils.h>
#include <spa/node/keys.h>
#include <spa/param/audio/format-utils.h>
#include <spa/param/audio/raw.h>
#include <spa/param/buffers.h>
#include <spa/param/audio/compressed.h>
#include <pipewire/impl.h>
#include <pipewire/i18n.h>
#include <PalApi.h>
#include <PalDefs.h>
#include <agm/agm_api.h>
#include <pthread.h>
#include "SoundTriggerUtils.h"


#define LOG_TAG "pw-pal-plugin"

#define BITS_PER_BYTE 8
#define NUM_BYTES ((SW_MAX + BITS_PER_BYTE) / BITS_PER_BYTE)
#define BIT_VALUE(bit, array) \
    ((array[(bit) / BITS_PER_BYTE] >> ((bit) % BITS_PER_BYTE)) & 1)

PW_LOG_TOPIC_STATIC(log_topic, "log:" LOG_TAG);
#define PW_LOG_TOPIC_DEFAULT log_topic

#define PW_DEFAULT_SAMPLE_FORMAT "S16"
#define PW_DEFAULT_SAMPLE_RATE 48000
#define PW_DEFAULT_SAMPLE_CHANNELS 2
#define PW_DEFAULT_SAMPLE_POSITION "[ FL FR ]"
#define PW_DEFAULT_BUFFER_DURATION_MS 25
#define PW_LOW_LATENCY_BUFFER_DURATION_MS 5
#define PW_DEEP_BUFFER_BUFFER_DURATION_MS 20
#define MAX_NAME_LENGTH 20
#define DEV_INPUT_DIR "/dev/input"
#define FILE_PREFIX "event"
#define MAX_DEVICES 4
#define PW_DEFAULT_CAPTURE_QUANTUM_FRAMES 1024
#define SVA_SOURCE_BUF_SIZE 512
#define SVA_SOURCE_BUF_COUNT 8
#define SVA_BIT_WIDTH 16
#define SVA_SAMPLE_RATE 16000
#define SVA_CHANNELS 1
#define SVA_LAB_DURATION 2000
#define SVA_FRAME_SIZE_BYTES 2
#define SVA_PHRASE_ID 1
#define SVA_NUM_SOUND_MODELS 3
#define SVA_PDK_CONFIDENCE_LEVEL 40
#define SVA_PDK_CONFIDENCE_LEVEL_DEFAULT 60
#define SVA_PREROLL_DURATION_MSEC 250
#define ST_CONFIG_VERSION_V2 0x2


enum sva_state{
        SVA_STATE_IDLE = 0,
        SVA_STATE_LOADED,
        SVA_STATE_ACTIVE,
        SVA_STATE_DETECTED,
};

struct sva_config {
    char model_path[256];       // sva.sound.model.path
    uint8_t *model_data;        // loaded binary (malloc'd)
    uint32_t model_size;        // computed from stat()
    uint32_t confidence_level;  // sva.confidence.level (0-100)
    uint32_t lab_duration;      // sva.lab.duration (ms, default 2000)
    bool lab_enabled;           // sva.lab.enabled (default true)
    char mode[8];               // sva.mode ("LPI" / "NLPI")
    uint32_t sample_rate;       // fixed SVA capture sample rate
    uint32_t channels;          // fixed SVA capture channel count
    uint32_t bit_width;         // fixed SVA capture bit width
};

struct sva_runtime {
      uint32_t detection_confidence;  // from detection event
      int32_t capture_delay_ms;       // signed, from pal_st_recognition_event
      int32_t capture_preamble_ms;
      bool capture_available;
      bool lab_draining;              // true while draining LAB buffer post-detection
      uint32_t lab_bytes_read;        // bytes drained so far
      uint32_t lab_bytes_total;       // target bytes = lab_duration worth of PCM
      /* Steady state ("idle"/"draining"/"buffered"/"delivering") is always
       * recomputed from the live flags below by sva_lab_status_name() —
       * never stored, so it can never go stale relative to those flags.
       * lab_event holds a one-shot transient value ("complete"/"error"/
       * "busy") that a publish consumes and clears; see
       * sva_publish_snapshot_on_loop(). */
      char lab_event[16];
      uint64_t last_detect_mono_ms;   // CLOCK_MONOTONIC stamp of most recent detection
      uint64_t last_rearm_mono_ms;    // CLOCK_MONOTONIC stamp PAL's history ring buffer
                                      // was last reset to empty (start/restart/reload) —
                                      // see sva_start_recognition() and the sva.restart
                                      // handler. PAL physically cannot return more LAB
                                      // history than has elapsed since this timestamp,
                                      // no matter what sva.lab.duration asks for.

      /* Single-slot LAB cache: holds the latest completed detection's LAB
       * PCM so a client that connects after the fact (pw-record,
       * pw-voiceui, or anything else) can still retrieve it. Overwritten
       * in place by the next completed detection, never accumulated. */
      uint8_t *lab_cache_data;        // malloc'd, sized to lab_bytes_total
      uint32_t lab_cache_capacity;    // current allocation size
      uint32_t lab_cache_size;        // valid bytes for the latest cached LAB
      uint32_t lab_cache_generation;  // incremented each time a new LAB finishes caching
      bool lab_cache_valid;           // true once a complete LAB has been cached
      bool lab_delivery_active;       // true while feeding a client from the cache
      uint32_t lab_delivery_offset;   // bytes of the cache already delivered
};

static uint64_t sva_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}


struct pw_userdata {
    struct pw_context *context;

    struct pw_properties *props;

    struct pw_impl_module *module;

    struct spa_hook module_listener;

    struct pw_core *core;
    struct spa_hook core_proxy_listener;
    struct spa_hook core_listener;

    struct pw_properties *stream_props;
    struct pw_stream *stream;
    struct spa_hook stream_listener;
    struct spa_hook sva_node_listener;
    bool sva_node_listener_added;
    struct spa_audio_info_raw info;
    uint32_t frame_size;
    struct spa_audio_info format;

    unsigned int do_disconnect:1;

    pal_stream_handle_t *stream_handle;
    struct pal_device *pal_device;
    struct pal_stream_attributes *stream_attributes;
    bool isplayback;
    pal_stream_type_t stream_type;
    pal_device_id_t pal_device_id[MAX_DEVICES];
    uint32_t no_of_devices;
    bool is_offload;

    pal_audio_fmt_t  codec_aud_fmt_id;
    uint32_t         codec_spa_subtype;
    uint32_t         codec_sample_rate;
    uint32_t         codec_channels;
    uint32_t         codec_bit_rate;

    size_t source_buf_size;
    size_t source_buf_count;
    size_t sink_buf_size;
    size_t sink_buf_count;
    bool pal_ready;
    bool pal_wait_logged;

    struct spa_source *jack_src;
    int jack_fd;
    char jack_name[MAX_NAME_LENGTH];

    /* playback starter thread */
    pthread_t       starter_thread;
    volatile bool   stream_ready;     /* set true once pal_stream_start returns */
    volatile bool   stop_requested;   /* set true when stream must stop */

    /* capture reader thread */
    pthread_t       reader_thread;
    bool            reader_running;
    uint8_t        *ring_buf;
    uint32_t        ring_size;      /* must be power-of-two */
    uint32_t        ring_write;     /* written by reader thread */
    uint32_t        ring_read;      /* read   by RT process()  */
    pthread_mutex_t ring_mutex;
    pthread_cond_t  ring_cond;

    bool is_sva;                        
    enum sva_state sva_state;           
    pal_stream_handle_t *sva_pal_handle;
    struct sva_config sva_cfg;
    struct sva_runtime sva_rt;    
    
};

static void sva_set_state(struct pw_userdata *udata, enum sva_state next_state);
static void sva_emit_props_changed(struct pw_userdata *udata);
static bool sva_lab_cycle_resolved(struct pw_userdata *udata);
static void pw_pal_set_volume(struct pw_userdata *udata, float gain);
static void *pw_pal_reader_thread(void *arg)
{
    struct pw_userdata *udata = arg;
    struct pal_buffer pal_buf;
    uint8_t *tmp;
    uint32_t buf_size = udata->source_buf_size;

    tmp = malloc(buf_size);
    if (!tmp) {
        pw_log_error("reader thread: malloc failed");
        return NULL;
    }

    pw_log_info("reader thread started, buf_size=%u ring_size=%u",
                buf_size, udata->ring_size);

    while (udata->reader_running) {
        memset(&pal_buf, 0, sizeof(pal_buf));
        pal_buf.buffer = tmp;
        pal_buf.size   = buf_size;

        int rc = pal_stream_read(udata->stream_handle, &pal_buf);
        if (rc < 0) {
            pw_log_error("reader thread: pal_stream_read error %d", rc);
            /* brief sleep to avoid busy-loop on persistent error */
            usleep(5000);
            continue;
        }

        uint32_t got = (pal_buf.size > 0) ? pal_buf.size : buf_size;
        if (pal_buf.size == 0)
            memset(tmp, 0, got); /* fill silence if PAL returned nothing */

        pthread_mutex_lock(&udata->ring_mutex);
        /* write into ring buffer; drop oldest data if full */
        uint32_t used = (udata->ring_write - udata->ring_read) &
                        (udata->ring_size - 1);
        uint32_t free_space = udata->ring_size - used - 1;
        if (got > free_space) {
            /* advance read pointer to make room (drop oldest) */
            udata->ring_read += (got - free_space);
        }
        uint32_t w = udata->ring_write & (udata->ring_size - 1);
        uint32_t tail = udata->ring_size - w;
        if (got <= tail) {
            memcpy(udata->ring_buf + w, tmp, got);
        } else {
            memcpy(udata->ring_buf + w, tmp, tail);
            memcpy(udata->ring_buf, tmp + tail, got - tail);
        }
        udata->ring_write += got;
        pthread_cond_signal(&udata->ring_cond);
        pthread_mutex_unlock(&udata->ring_mutex);
    }

    free(tmp);
    pw_log_info("reader thread exited");
    return NULL;
}


static void *pw_pal_starter_thread(void *arg)
{
    struct pw_userdata *udata = arg;
    int rc;

    pw_log_info("starter thread: calling pal_stream_start");
    rc = pal_stream_start(udata->stream_handle);
    if (rc)
        pw_log_warn("starter thread: pal_stream_start failed %d", rc);
    else
        pw_log_info("starter thread: pal_stream_start done, stream ready");
    if (!rc && !udata->stop_requested)
        pw_pal_set_volume(udata, 1.0);

    /* volume set must happen after stream is started */
    __sync_synchronize();          /* memory barrier before flag publish */
    udata->stream_ready = true;
    return NULL;
}


static int close_pal_stream(struct pw_userdata *udata);
static void sva_schedule_snapshot(struct pw_userdata *udata);

static void pw_pal_destroy_stream(void *d)
{
    struct pw_userdata *udata = d;

    spa_hook_remove(&udata->stream_listener);
    udata->stream = NULL;
}

static int32_t pa_pal_out_cb(pal_stream_handle_t *stream_handle,
                            uint32_t event_id, uint32_t *event_data,
                            uint32_t event_size, uint64_t cookie) {
    struct pw_userdata *udata = (struct pw_userdata *)cookie;

    if (udata == NULL)
        return 0;

    udata->pal_ready = true;
    udata->pal_wait_logged = false;

    return 0;
}
static void pw_pal_set_volume (struct pw_userdata *udata, float gain)
{
    int rc = 0, i;
    uint32_t channel_mask = 1;
    uint32_t no_vol_pair = udata->stream_attributes->out_media_config.ch_info.channels;
    struct pal_volume_data *volume = (struct pal_volume_data *)malloc(sizeof(uint32_t) +
        (sizeof(struct pal_channel_vol_kv) * (no_vol_pair)));
    for (i = 0; i < no_vol_pair; i++)
        channel_mask = (channel_mask | udata->stream_attributes->out_media_config.ch_info.ch_map[i]);
    channel_mask = (channel_mask << 1);
    if (volume) {
        volume->no_of_volpair = no_vol_pair;
        for (i = 0; i < no_vol_pair; i++) {
            volume->volume_pair[i].channel_mask = channel_mask;
            volume->volume_pair[i].vol = gain;
        }
        rc = pal_stream_set_volume(udata->stream_handle, volume);
        if (rc)
            pw_log_error("pal_stream_set_volume failed, error %d\n", rc);
        free(volume);
    }
}


/* ═══════════════════════════════════════════════════════════════
 * SVA PAL stream callback — receives detection events from DSP
 * ═══════════════════════════════════════════════════════════════ */

/*
 * Steady-state LAB status, recomputed fresh from live flags every time it's
 * needed — never stored, so a stale value can never survive past the event
 * that made it true.
 */
static const char *sva_lab_status_name(struct pw_userdata *udata)
{
    if (udata->sva_rt.lab_draining)        return "draining";
    if (udata->sva_rt.lab_delivery_active) return "delivering";
    if (udata->sva_rt.lab_cache_valid)     return "buffered";
    return "idle";
}

/*
 * Single entry point for starting delivery from the cache, used by both the
 * auto-arm triggers (drain-complete, STREAMING) and the explicit
 * sva.lab.acquire request. Refuses if a drain or a delivery is already in
 * flight, or if there is nothing cached yet — callers branch on the return
 * value to decide whether to report "busy" or just leave the steady state
 * as-is.
 */
static bool sva_lab_try_auto_start(struct pw_userdata *udata)
{
    if (udata->sva_rt.lab_draining || udata->sva_rt.lab_delivery_active)
        return false;
    if (!udata->sva_rt.lab_cache_valid)
        return false;
    udata->sva_rt.lab_delivery_active = true;
    udata->sva_rt.lab_delivery_offset = 0;
    return true;
}

struct sva_peer_scan {
    struct pw_impl_node *own_node;
    bool have_recorder_peer; /* a linked peer that is NOT a VoiceUI-role listener */
};

static int sva_peer_scan_cb(void *data, struct pw_global *global)
{
    struct sva_peer_scan *scan = data;
    struct pw_impl_link *link;
    struct pw_impl_port *out, *in;
    struct pw_impl_node *peer_node;
    const struct pw_properties *props;
    const char *role;

    if (!pw_global_is_type(global, PW_TYPE_INTERFACE_Link))
        return 0;
    link = pw_global_get_object(global);
    if (!link)
        return 0;
    out = pw_impl_link_get_output(link);
    if (!out || pw_impl_port_get_node(out) != scan->own_node)
        return 0; /* not one of our node's links */
    in = pw_impl_link_get_input(link);
    peer_node = in ? pw_impl_port_get_node(in) : NULL;
    props = peer_node ? pw_impl_node_get_properties(peer_node) : NULL;
    role = props ? pw_properties_get(props, PW_KEY_MEDIA_ROLE) : NULL;
    pw_log_warn("[SVA] peer scan: node.name=%s media.role=%s -> %s",
                props ? pw_properties_get(props, PW_KEY_NODE_NAME) : "(null)",
                role ? role : "(null)",
                (role && spa_streq(role, "VoiceUI")) ? "listener" : "recorder");
    if (!(role && spa_streq(role, "VoiceUI")))
        scan->have_recorder_peer = true;
    return 0; /* keep scanning — need the full link set, not just the first */
}

/*
 * Resolves this plugin's own exported node to the internal pw_impl_node
 * object. Needed both to walk this node's current links
 * (sva_has_recorder_peer()) and to listen for new ones (peer_added, see
 * sva_node_peer_added() below) — the public pw_stream API exposes neither.
 */
static struct pw_impl_node *sva_resolve_own_node(struct pw_userdata *udata)
{
    uint32_t node_id;
    struct pw_global *global;

    if (!udata->stream)
        return NULL;
    node_id = pw_stream_get_node_id(udata->stream);
    if (node_id == SPA_ID_INVALID)
        return NULL;
    global = pw_context_find_global(udata->context, node_id);
    return global ? pw_global_get_object(global) : NULL;
}

/*
 * pw-voiceui's persistent `listen` session connects with
 * PW_KEY_MEDIA_ROLE="VoiceUI" (see pw-voiceui.c's stream properties at
 * connect time) specifically to receive status/detection notifications —
 * it is not an implicit "give me the cached LAB" request the way a bare
 * pw-record connection is. Auto-arming delivery for it anyway occupies
 * the single delivery slot and can make a subsequent *explicit*
 * sva.lab.acquire ("replay") come back "busy". This walks the node's
 * current links (public pw_context_for_each_global API only — no
 * private.h needed) and reports whether at least one connected peer is
 * something other than a VoiceUI listener (e.g. pw-record, which has no
 * SVA prop vocabulary and so can ONLY ever receive LAB via this implicit
 * path). sva.lab.acquire itself always calls sva_lab_try_auto_start()
 * directly and is unaffected by this gate.
 */
static bool sva_has_recorder_peer(struct pw_userdata *udata)
{
    struct pw_impl_node *own_node = sva_resolve_own_node(udata);
    struct sva_peer_scan scan;

    if (!own_node) {
        pw_log_warn("[SVA] peer scan: couldn't resolve own node — failing open (recorder assumed)");
        return true; /* can't determine — fail open, don't block pw-record */
    }

    scan.own_node = own_node;
    scan.have_recorder_peer = false;
    pw_context_for_each_global(udata->context, sva_peer_scan_cb, &scan);
    pw_log_warn("[SVA] peer scan: own node=%p -> have_recorder_peer=%s",
                own_node, scan.have_recorder_peer ? "true" : "false");
    return scan.have_recorder_peer;
}

/*
 * peer_added fires on OUR node (via PipeWire's own pw_impl_node_add_target(),
 * impl-node.c) whenever a new link to us actually activates — regardless of
 * whether our own pw_stream was already STREAMING. This is the piece that
 * was missing: pw_pal_change_stream_state()'s STREAMING case fires exactly
 * once, for whichever client causes our node to first start (typically
 * pw-voiceui's long-lived `listen` session) — it does NOT fire again when a
 * second client (e.g. pw-record) links in afterward while we're already
 * streaming. Without this listener, a client that connects after pw-voiceui
 * is already attached never gets the auto-arm check run for it at all, even
 * though the cache may already be sitting there "buffered".
 *
 * Catch recorder peers that connect after the SVA stream is already
 * streaming.
 */
static void sva_node_peer_added(void *data, struct pw_impl_node *peer)
{
    struct pw_userdata *udata = data;
    const struct pw_properties *props = pw_impl_node_get_properties(peer);
    const char *role = props ? pw_properties_get(props, PW_KEY_MEDIA_ROLE) : NULL;

    pw_log_warn("[SVA] peer added: node.name=%s media.role=%s -> %s",
                props ? pw_properties_get(props, PW_KEY_NODE_NAME) : "(null)",
                role ? role : "(null)",
                (role && spa_streq(role, "VoiceUI")) ? "listener" : "recorder");

    if (role && spa_streq(role, "VoiceUI"))
        return; /* a VoiceUI listener attaching is not an implicit LAB request */

    if (sva_lab_try_auto_start(udata))
        sva_schedule_snapshot(udata);
}

static const struct pw_impl_node_events sva_node_events = {
    PW_VERSION_IMPL_NODE_EVENTS,
    .peer_added = sva_node_peer_added,
};

/*
 * Registers sva_node_peer_added() on our own node, once its pw_impl_node
 * becomes resolvable. Safe/cheap to call repeatedly — no-ops once
 * sva_node_listener_added is set. Called from the STREAMING case (the
 * earliest point the node id is reliably resolvable), so both triggers
 * are covered: a first client's own connect still gets the inline
 * sva_has_recorder_peer() check right after this, and every later client's
 * connect is caught by the listener this installs.
 */
static void sva_ensure_peer_listener(struct pw_userdata *udata)
{
    struct pw_impl_node *own_node;

    if (udata->sva_node_listener_added)
        return;
    own_node = sva_resolve_own_node(udata);
    if (!own_node)
        return;
    pw_impl_node_add_listener(own_node, &udata->sva_node_listener,
                              &sva_node_events, udata);
    udata->sva_node_listener_added = true;
}

/*
 * PipeWire caches exactly ONE Props value per param id on a node — a
 * later pw_stream_update_params() call REPLACES it, it does not merge
 * with whatever an earlier call published. With five independent publish
 * helpers (state-only, detection-only, lab-status-only, idle-sync,
 * replay-start) a later narrow publish could silently blank out keys an
 * earlier one had just set — e.g. sva.restart's state=active getting
 * overwritten moments later by a detection/lab idle-sync pod that carries
 * no sva.state key at all. That produced "state: unknown" in `status`
 * and restart/reset appearing to do nothing on another connected
 * terminal. Every state-changing call site below now funnels through
 * this single function, which always builds a complete, self-consistent
 * snapshot straight from live udata fields — never a partial diff.
 */
static int sva_publish_snapshot_on_loop(struct spa_loop *loop, bool async,
                                        uint32_t seq, const void *data,
                                        size_t size, void *user_data)
{
    struct pw_userdata *udata = user_data;
    static const char *state_names[] = { "idle", "loaded", "active", "detected" };
    uint8_t pod_buf[2048];
    struct spa_pod_builder b = SPA_POD_BUILDER_INIT(pod_buf, sizeof(pod_buf));
    struct spa_pod_frame f[2];
    const struct spa_pod *pod;
    int rc;

    (void)loop; (void)async; (void)seq; (void)data; (void)size;
    if (!udata || !udata->stream)
        return -EINVAL;

    spa_pod_builder_push_object(&b, &f[0], SPA_TYPE_OBJECT_Props, SPA_PARAM_Props);
    spa_pod_builder_prop(&b, SPA_PROP_params, 0);
    spa_pod_builder_push_struct(&b, &f[1]);

    spa_pod_builder_string(&b, "sva.state");
    spa_pod_builder_string(&b, state_names[udata->sva_state]);

    /*
     * Order matters: the client parses this POD key-by-key in a single
     * pass, and some of its handlers read sibling fields that must
     * already be current for THIS snapshot — e.g. the lab.status handler
     * reads the byte-count fields, and the detection.status=="detected"
     * handler announces confidence/capture_*. Every field a handler
     * depends on is emitted ahead of the field whose handler reads it;
     * detection.status and lab.status deliberately go last in their
     * respective groups.
     */
    spa_pod_builder_string(&b, "sva.detection.confidence");
    spa_pod_builder_int(&b, (int32_t)udata->sva_rt.detection_confidence);
    spa_pod_builder_string(&b, "sva.detection.capture_delay_ms");
    spa_pod_builder_int(&b, udata->sva_rt.capture_delay_ms);
    spa_pod_builder_string(&b, "sva.detection.capture_preamble_ms");
    spa_pod_builder_int(&b, udata->sva_rt.capture_preamble_ms);
    spa_pod_builder_string(&b, "sva.lab.duration");
    spa_pod_builder_int(&b, (int32_t)udata->sva_cfg.lab_duration);
    /*
     * capture_available means PAL has LAB ready to hand over — delivery
     * itself now starts automatically once a client is connected and the
     * cache is buffered (see sva_lab_try_auto_start() and its call sites
     * in pw_pal_process_stream()/pw_pal_change_stream_state()). pw-record
     * receives it the same way any other connected client does: no
     * sva.* prop is required from it. sva.lab.acquire remains available
     * as an internal-only way to manually re-trigger delivery of the
     * still-buffered cache (used by pw-voiceui's `replay` command).
     */
    spa_pod_builder_string(&b, "sva.detection.capture_available");
    spa_pod_builder_bool(&b, udata->sva_rt.capture_available);
    spa_pod_builder_string(&b, "sva.detection.status");
    spa_pod_builder_string(&b, udata->sva_state == SVA_STATE_DETECTED ? "detected" : "idle");

    spa_pod_builder_string(&b, "sva.lab.bytes");
    spa_pod_builder_long(&b, (int64_t)udata->sva_rt.lab_bytes_read);
    spa_pod_builder_string(&b, "sva.lab.requested_bytes");
    spa_pod_builder_long(&b, (int64_t)udata->sva_rt.lab_bytes_total);
    spa_pod_builder_string(&b, "sva.lab.cached_bytes");
    spa_pod_builder_long(&b, (int64_t)(udata->sva_rt.lab_cache_valid ?
                                        udata->sva_rt.lab_cache_size : 0));
    spa_pod_builder_string(&b, "sva.lab.delivered_bytes");
    spa_pod_builder_long(&b, (int64_t)udata->sva_rt.lab_delivery_offset);
    spa_pod_builder_string(&b, "sva.lab.cache_generation");
    spa_pod_builder_long(&b, (int64_t)udata->sva_rt.lab_cache_generation);
    /*
     * sva.lab.status is the last field emitted since every byte-count
     * field above it is part of "this" snapshot. lab_event is a one-shot
     * transient ("complete"/"error"/"busy") that this publish consumes
     * and clears — callers that set it must publish twice in a row (once
     * for the event, once more right after for the now-correct recomputed
     * steady state) so a later subscriber never observes a stale
     * transient as the node's current status. */
    spa_pod_builder_string(&b, "sva.lab.status");
    spa_pod_builder_string(&b, udata->sva_rt.lab_event[0] ?
                                udata->sva_rt.lab_event : sva_lab_status_name(udata));
    udata->sva_rt.lab_event[0] = '\0';

    spa_pod_builder_pop(&b, &f[1]);
    pod = spa_pod_builder_pop(&b, &f[0]);
    if (!pod) {
        pw_log_error("[SVA] snapshot POD build failed (buffer overflow, size=%zu)",
                     sizeof(pod_buf));
        return -EINVAL;
    }
    rc = pw_stream_update_params(udata->stream, &pod, 1);
    if (rc < 0)
        pw_log_error("[SVA] snapshot publication failed: %s", spa_strerror(rc));
    return rc;
}

/*
 * pw_loop_invoke() runs synchronously (returning the real, possibly
 * negative, result of sva_publish_snapshot_on_loop()) when called from
 * the main loop's own thread — true for every sva.* prop handler, since
 * those run as part of pw_stream's param_changed callback. Called from
 * any other thread (the PAL callback thread, for live detections) it
 * queues the work and returns SPA_RESULT_RETURN_ASYNC(seq), which is
 * always >= 0 — so "< 0" here only ever flags a genuine failure, never
 * a successful async enqueue.
 */
static void sva_schedule_snapshot(struct pw_userdata *udata)
{
    int rc;
    if (!udata || !udata->context)
        return;
    rc = pw_loop_invoke(pw_context_get_main_loop(udata->context), sva_publish_snapshot_on_loop,
                        0, NULL, 0, false, udata);
    if (rc < 0)
        pw_log_error("[SVA] failed to schedule snapshot publication: %s", spa_strerror(rc));
}

/*
 * Everything sva_pal_stream_callback() learns from a single PAL event,
 * copied out of PAL's (call-scoped, thread-foreign) event_data buffer so
 * it can be safely handed across to the main loop thread via
 * pw_loop_invoke()'s own data-copying queue.
 */
struct sva_detection_payload {
    pal_stream_handle_t *stream_handle;
    uint32_t             confidence;
    int32_t              capture_delay_ms;
    int32_t              capture_preamble_ms;
    bool                 capture_available;
};

/*
 * Runs on the main loop thread (invoked via pw_loop_invoke() from
 * sva_pal_stream_callback(), which runs on PAL's own detection-callback
 * thread). pw_pal_process_stream() and pw_pal_change_stream_param()
 * already run exclusively on the main loop thread (is_sva streams never
 * set PW_STREAM_FLAG_RT_PROCESS, see pw_pal_create_stream()) and never
 * race each other; the only other writer of sva_rt is this detection
 * handler, which otherwise runs on a foreign PAL thread with zero
 * synchronization against them. Marshaling the entire handler onto the
 * main loop — rather than adding a mutex sprinkled across every access
 * site — makes every sva_rt access single-threaded, including the
 * udata->sva_pal_handle staleness check below (otherwise racing
 * close_pal_stream() on the main thread; here it runs on that same
 * thread, so the read can never see a torn/concurrently-mutated value).
 */
static int sva_handle_detection_on_loop(struct spa_loop *loop, bool async, uint32_t seq,
                                         const void *data, size_t size, void *user_data)
{
    struct pw_userdata *udata = user_data;
    const struct sva_detection_payload *ev = data;

    (void)loop; (void)async; (void)seq; (void)size;

    /*
     * close_pal_stream() (sva.reset handler, disconnect path) can run
     * before a PAL callback that was already queued/in-flight before the
     * close is processed here — PAL's own mStreamMutex only protects
     * PAL's internal state, not this plugin's udata. Once
     * close_pal_stream() has stopped+closed the handle and nulled
     * sva_pal_handle, any callback still queued for the OLD handle must
     * be dropped, or it re-dirties detection/LAB state and fires
     * sva_set_state(DETECTED) after a reset already published IDLE.
     *
     * Drop callbacks belonging to a PAL stream that has already been
     * reset or closed. */
    if (!udata->sva_pal_handle || ev->stream_handle != udata->sva_pal_handle) {
        pw_log_warn("[SVA] pal_stream_callback: stale callback for handle %p "
                    "(current=%p) — dropping, stream was reset/closed",
                    (void *)ev->stream_handle, (void *)udata->sva_pal_handle);
        return 0;
    }

    /* Store in runtime state */
    udata->sva_rt.detection_confidence  = ev->confidence;
    udata->sva_rt.capture_delay_ms      = ev->capture_delay_ms;
    udata->sva_rt.capture_preamble_ms   = ev->capture_preamble_ms;
    udata->sva_rt.capture_available     = ev->capture_available;
    udata->sva_rt.last_detect_mono_ms = sva_now_ms();
    pw_log_warn("[SVA] detection callback entry: mono_ts=%llu ms",
                (unsigned long long)udata->sva_rt.last_detect_mono_ms);

    /*
     * A live detection arms the background PAL drain into our own cache
     * (below) so delivery can start the moment it completes (see
     * sva_lab_try_auto_start(), called from pw_pal_process_stream() on
     * drain-complete and from pw_pal_change_stream_state() on STREAMING).
     * Only one delivery/drain is active at a time — if a delivery from a
     * previous cache is still in flight, this detection's capture is
     * skipped rather than disturbing it.
     */
    bool drain_armed = false;
    if (udata->sva_rt.lab_delivery_active) {
        pw_log_warn("[SVA] detection fired while a LAB delivery is in progress — "
                    "skipping capture for this cycle; the in-progress "
                    "delivery is left untouched");
    } else if (udata->sva_cfg.lab_enabled && ev->capture_available) {
          uint32_t bytes_per_ms = (udata->sva_cfg.sample_rate * udata->sva_cfg.channels *
                                    (udata->sva_cfg.bit_width / 8)) / 1000;
          /*
           * sva.lab.duration is only bounded to ">0" at the setter — an
           * attacker-or-typo-supplied large value here could overflow the
           * uint32_t product and wrap to a small allocation, after which
           * the drain/delivery paths would write past lab_cache_capacity.
           * Compute in 64-bit and reject rather than silently truncating.
           */
          uint64_t new_bytes_total_64 = (uint64_t)bytes_per_ms * udata->sva_cfg.lab_duration;
          bool have_capacity;
          uint32_t new_bytes_total = 0;
          if (new_bytes_total_64 > UINT32_MAX) {
              pw_log_error("[SVA] LAB duration %u ms would need %llu bytes — "
                           "overflows uint32_t, dropping this capture's cache",
                           udata->sva_cfg.lab_duration, (unsigned long long)new_bytes_total_64);
              have_capacity = false;
          } else {
          new_bytes_total = (uint32_t)new_bytes_total_64;

          /* Grow the single-slot LAB cache only when the current
           * allocation is too small; a same-size-or-smaller detection
           * reuses the existing buffer rather than reallocating.
           *
           * Do this BEFORE touching lab_cache_valid/lab_draining: if the
           * previous cache is still valid and this realloc fails, we must
           * keep serving the old cache rather than dropping it for a new
           * capture that can't actually happen. */
          have_capacity = udata->sva_rt.lab_cache_capacity >= new_bytes_total;
          if (!have_capacity) {
              uint8_t *grown = realloc(udata->sva_rt.lab_cache_data, new_bytes_total);
              if (grown) {
                  udata->sva_rt.lab_cache_data     = grown;
                  udata->sva_rt.lab_cache_capacity = new_bytes_total;
                  have_capacity = true;
              } else {
                  pw_log_error("[SVA] LAB cache realloc failed (%u bytes) — "
                               "keeping previous cache (valid=%s), this "
                               "detection's LAB will not be captured",
                               new_bytes_total,
                               udata->sva_rt.lab_cache_valid ? "true" : "false");
              }
          }
          }

          if (have_capacity) {
              udata->sva_rt.lab_bytes_total = new_bytes_total;
              udata->sva_rt.lab_bytes_read  = 0;
              udata->sva_rt.lab_draining    = true;
              udata->sva_rt.lab_cache_valid = false;
              udata->sva_rt.lab_cache_size  = 0;
              drain_armed = true;

              pw_log_warn("[SVA] LAB drain armed: target=%u bytes (%u ms)",
                          udata->sva_rt.lab_bytes_total, udata->sva_cfg.lab_duration);
          } else {
              udata->sva_rt.lab_draining = false;
              if (!udata->sva_rt.lab_cache_valid)
                  snprintf(udata->sva_rt.lab_event, sizeof(udata->sva_rt.lab_event), "error");
          }

    } else {
          udata->sva_rt.lab_draining = false;
    }

    /* Update state machine — sva_set_state() already schedules a snapshot
     * publish internally; calling sva_schedule_snapshot() again here would
     * publish the identical snapshot twice, causing clients to see a
     * duplicate detection edge (e.g. two "KEYWORD DETECTED!" banners). The
     * exception is lab_event: that first publish consumes/clears it, so if
     * it was set above we need one more explicit publish afterward so a
     * later subscriber sees the corrected steady state, not the transient. */
    bool lab_had_event = udata->sva_rt.lab_event[0] != '\0';
    sva_set_state(udata, SVA_STATE_DETECTED);
    if (lab_had_event)
        sva_schedule_snapshot(udata);

    /* If this detection never armed a drain (LAB disabled, capture
     * unavailable, overlapping delivery, or no cache capacity), its LAB
     * cycle is already fully resolved right here — nothing downstream in
     * pw_pal_process_stream() will ever run for it. Revert out of
     * SVA_STATE_DETECTED now so a stale "detected" doesn't outlive this
     * function. When a drain *was* armed, pw_pal_process_stream() calls
     * sva_lab_cycle_resolved() itself once that drain/delivery concludes. */
    if (!drain_armed)
        sva_lab_cycle_resolved(udata);

    return 0;
}

/*
 * PAL's detection-callback thread entry point. Only reads event_data
 * (call-scoped, owned by PAL — invalid once this function returns, so it
 * cannot be deferred) and logs; touches no udata->sva_rt field directly.
 * All state mutation is marshaled onto the main loop via
 * sva_handle_detection_on_loop() above — see that function's comment for
 * why.
 */
static int32_t sva_pal_stream_callback(pal_stream_handle_t *stream_handle,
                                    uint32_t event_id,
                                    uint32_t *event_data,
                                    uint32_t event_data_size,
                                    uint64_t cookie)
{
    struct pw_userdata *udata = (struct pw_userdata *)cookie;

    pw_log_debug("[SVA] pal_stream_callback event_id=%u event_data=%p event_data_size=%u",
                event_id, (void *)event_data, event_data_size);

    if (!udata || !event_data) {
        pw_log_warn("[SVA] pal_stream_callback: NULL udata or event_data");
        return 0;
    }

    /* PAL always passes event_id=0 for recognition events today (see
     * StreamSoundTrigger::notifyClient()) — the real discriminator is
     * event->status below. event_data_size must at least cover the
     * common header before we dereference any of its fields. */
    if (event_data_size < sizeof(struct pal_st_recognition_event)) {
        pw_log_error("[SVA] pal_stream_callback: event_data_size=%u too small for "
                     "pal_st_recognition_event (need %zu) — dropping event",
                     event_data_size, sizeof(struct pal_st_recognition_event));
        return 0;
    }

    struct pal_st_phrase_recognition_event *phrase_event =
        (struct pal_st_phrase_recognition_event *)((void *)event_data);
    struct pal_st_recognition_event *event = &phrase_event->common;

    if (event->status == PAL_RECOGNITION_STATUS_ABORT) {
        pw_log_warn("[SVA] pal_stream_callback: recognition aborted by PAL "
                    "(status=PAL_RECOGNITION_STATUS_ABORT) — not a detection");
        return 0;
    }

    pw_log_warn("[SVA] ════════════════════════════════════════");
    pw_log_warn("[SVA] *** KEYWORD DETECTED ***");
    pw_log_warn("[SVA]   status              = %d", event->status);
    pw_log_warn("[SVA]   type                = %d", (int)event->type);
    pw_log_warn("[SVA]   capture_available   = %s",
                event->capture_available ? "true" : "false");
    pw_log_warn("[SVA]   capture_delay_ms    = %d", event->capture_delay_ms);
    pw_log_warn("[SVA]   capture_preamble_ms = %d", event->capture_preamble_ms);
    pw_log_warn("[SVA]   data_size           = %u", event->data_size);
    pw_log_warn("[SVA]   media_config.sr     = %u", event->media_config.sample_rate);
    pw_log_warn("[SVA]   media_config.ch     = %u", event->media_config.ch_info.channels);

    /* Extract confidence from phrase_extras[] */
    uint32_t conf = 0;

    if (event->type == PAL_SOUND_MODEL_TYPE_KEYPHRASE &&
        event_data_size >= sizeof(struct pal_st_phrase_recognition_event) &&
        phrase_event->num_phrases > 0 &&
        phrase_event->num_phrases <= PAL_SOUND_TRIGGER_MAX_PHRASES) {
        conf = phrase_event->phrase_extras[0].confidence_level;
        pw_log_warn("[SVA]   phrase[0].id         = %u",
                    phrase_event->phrase_extras[0].id);
        pw_log_warn("[SVA]   phrase[0].confidence = %u%%", conf);
    }

    pw_log_warn("[SVA] ════════════════════════════════════════");

    struct sva_detection_payload payload = {
        .stream_handle       = stream_handle,
        .confidence           = conf,
        .capture_delay_ms     = event->capture_delay_ms,
        .capture_preamble_ms  = event->capture_preamble_ms,
        .capture_available    = event->capture_available,
    };

    /* Async (block=false): PAL doesn't need to wait for us to finish
     * updating sva_rt / publishing the snapshot before its callback
     * returns. pw_loop_invoke() copies `payload` into its own queue, so
     * it stays valid for sva_handle_detection_on_loop() even though this
     * stack frame is gone by the time the main loop gets to it. */
    int rc = pw_loop_invoke(pw_context_get_main_loop(udata->context),
                            sva_handle_detection_on_loop, 0,
                            &payload, sizeof(payload), false, udata);
    if (rc < 0)
        pw_log_error("[SVA] failed to schedule detection handling: %s", spa_strerror(rc));

    return 0;
}


static int sva_set_recognition_config(struct pw_userdata *udata)
{
    uint32_t num_phrases = 1;
    uint32_t opaque_size = sizeof(struct st_param_header) +
                        sizeof(struct st_confidence_levels_info_v2) +
                        sizeof(struct st_param_header) +
                        sizeof(struct st_hist_buffer_info);
    uint32_t rc_config_size = sizeof(struct pal_st_recognition_config) + opaque_size;

    pal_param_payload *rec_payload = calloc(1, sizeof(pal_param_payload) + rc_config_size);
    if (!rec_payload) return -ENOMEM;

    /* Match the original working PAL implementation: the recognition payload
     * size includes the outer pal_param_payload header. */
    rec_payload->payload_size = sizeof(pal_param_payload) + rc_config_size;
    struct pal_st_recognition_config *rec_cfg =
        (struct pal_st_recognition_config *)rec_payload->payload;

    rec_cfg->capture_handle    = 0;
    rec_cfg->capture_device    = PAL_DEVICE_IN_HANDSET_VA_MIC;
    rec_cfg->capture_requested = udata->sva_cfg.lab_enabled ? 1 : 0;
    rec_cfg->num_phrases       = num_phrases;
    rec_cfg->data_size         = opaque_size;
    rec_cfg->data_offset       = sizeof(struct pal_st_recognition_config);
    rec_cfg->callback          = NULL;
    rec_cfg->cookie            = (uint8_t *)udata;
    rec_cfg->phrases[0].id               = SVA_PHRASE_ID;
    rec_cfg->phrases[0].recognition_modes = PAL_RECOGNITION_MODE_VOICE_TRIGGER;
    rec_cfg->phrases[0].confidence_level  = udata->sva_cfg.confidence_level;
    rec_cfg->phrases[0].num_levels        = 0;

    uint8_t *payload_ptr = (uint8_t *)rec_cfg + rec_cfg->data_offset;
    struct st_param_header *hdr = (struct st_param_header *)payload_ptr;
    hdr->key_id       = ST_PARAM_KEY_CONFIDENCE_LEVELS;
    hdr->payload_size = sizeof(struct st_confidence_levels_info_v2);
    payload_ptr += sizeof(struct st_param_header);

    struct st_confidence_levels_info_v2 *conf_info =
        (struct st_confidence_levels_info_v2 *)payload_ptr;
    conf_info->version          = ST_CONFIG_VERSION_V2;
    conf_info->num_sound_models = SVA_NUM_SOUND_MODELS;
    conf_info->conf_levels[0].sm_id = ST_SM_ID_SVA_F_STAGE_GMM;
    conf_info->conf_levels[0].num_kw_levels = num_phrases;
    conf_info->conf_levels[0].kw_levels[0].kw_level = (int32_t)udata->sva_cfg.confidence_level;
    conf_info->conf_levels[0].kw_levels[0].num_user_levels = 0;
    conf_info->conf_levels[1].sm_id = ST_SM_ID_SVA_S_STAGE_PDK;
    conf_info->conf_levels[1].num_kw_levels = num_phrases;
    conf_info->conf_levels[1].kw_levels[0].kw_level = SVA_PDK_CONFIDENCE_LEVEL;
    conf_info->conf_levels[1].kw_levels[0].num_user_levels = 0;
    conf_info->conf_levels[2].sm_id = ST_SM_ID_SVA_S_STAGE_USER;
    conf_info->conf_levels[2].num_kw_levels = num_phrases;
    conf_info->conf_levels[2].kw_levels[0].kw_level = 0;
    conf_info->conf_levels[2].kw_levels[0].num_user_levels = 0;
    payload_ptr += sizeof(struct st_confidence_levels_info_v2);

    hdr = (struct st_param_header *)payload_ptr;
    hdr->key_id       = ST_PARAM_KEY_HISTORY_BUFFER_CONFIG;
    hdr->payload_size = sizeof(struct st_hist_buffer_info);
    payload_ptr += sizeof(struct st_param_header);
    struct st_hist_buffer_info *hist = (struct st_hist_buffer_info *)payload_ptr;
    hist->version                   = ST_CONFIG_VERSION_V2;
    /*
     * PAL only buffers as much history as it's told to here — this is
     * not a read-only ceiling, it's the actual ask. Using the fixed
     * SVA_HIST_BUFFER_DURATION_MSEC default regardless of the
     * caller-requested sva.lab.duration silently truncated every LAB
     * capture to 1750ms no matter what duration was configured. Request
     * exactly what was asked for so a client requesting N ms of LAB
     * always gets N ms back.
     */
    hist->hist_buffer_duration_msec = udata->sva_cfg.lab_duration;
    hist->pre_roll_duration_msec    = SVA_PREROLL_DURATION_MSEC;

    int rc = pal_stream_set_param(udata->sva_pal_handle,
                                PAL_PARAM_ID_RECOGNITION_CONFIG, rec_payload);
    free(rec_payload);
    return rc;
}

/* ═══════════════════════════════════════════════════════════════
 * sva_load_sound_model() — Open PAL + Load Keyphrase Model
 * ═══════════════════════════════════════════════════════════════ */
static int sva_load_sound_model(struct pw_userdata *udata)
{
    int rc = 0;
    struct stat st;
    FILE *fp = NULL;

    /* 1. Read model binary from filesystem */
    if (stat(udata->sva_cfg.model_path, &st) != 0) {
        pw_log_error("SVA: Cannot stat model: %s (%m)", udata->sva_cfg.model_path);
        return -errno;
    }

    if (st.st_size <= 0 || st.st_size > UINT32_MAX) {
        pw_log_error("SVA: model %s has invalid size %lld",
                      udata->sva_cfg.model_path, (long long)st.st_size);
        return -EINVAL;
    }
    udata->sva_cfg.model_size = (uint32_t)st.st_size;

    if (udata->sva_cfg.model_data) {
        free(udata->sva_cfg.model_data);
        udata->sva_cfg.model_data = NULL;
    }

    udata->sva_cfg.model_data = malloc(udata->sva_cfg.model_size);
    if (!udata->sva_cfg.model_data) return -ENOMEM;

    fp = fopen(udata->sva_cfg.model_path, "rb");
    if (!fp) {
        free(udata->sva_cfg.model_data);
        udata->sva_cfg.model_data = NULL;
        return -errno;
    }

    if (fread(udata->sva_cfg.model_data, 1, udata->sva_cfg.model_size, fp)
        != udata->sva_cfg.model_size) {
        pw_log_error("SVA: Short read on model file");
        fclose(fp);
        free(udata->sva_cfg.model_data);
        udata->sva_cfg.model_data = NULL;
        return -EIO;
    }
    fclose(fp);

    /* 2. Open PAL Stream — WITH callback for detection events */

    if (udata->stream_attributes) {
        free(udata->stream_attributes);
        udata->stream_attributes = NULL;
    }
    udata->stream_attributes = calloc(1, sizeof(struct pal_stream_attributes));
    if (udata->stream_attributes == NULL) {
        pw_log_error("SVA: failed to allocate stream attributes: %m");
        return -ENOMEM;
    }
    struct pal_stream_attributes *stream_attr = udata->stream_attributes;
    memset(stream_attr, 0, sizeof(*stream_attr));
    stream_attr->type                                = PAL_STREAM_VOICE_UI;
    stream_attr->info.voice_rec_info.version         = 1;
    stream_attr->flags                               = 0;
    stream_attr->direction                           = PAL_AUDIO_INPUT;
    stream_attr->in_media_config.sample_rate         = udata->sva_cfg.sample_rate ? udata->sva_cfg.sample_rate : SVA_SAMPLE_RATE;
    stream_attr->in_media_config.bit_width           = SVA_BIT_WIDTH;
    stream_attr->in_media_config.aud_fmt_id          = PAL_AUDIO_FMT_PCM_S16_LE;
    stream_attr->in_media_config.ch_info.channels    = udata->sva_cfg.channels ? udata->sva_cfg.channels : SVA_CHANNELS;
    stream_attr->in_media_config.ch_info.ch_map[0]   = PAL_CHMAP_CHANNEL_FL;

    /* Build pal_device fresh */
    if (udata->pal_device) {
        free(udata->pal_device);
        udata->pal_device = NULL;
    }
    udata->no_of_devices = 1;
    udata->pal_device = calloc(1, sizeof(struct pal_device));
    if (udata->pal_device == NULL) {
        pw_log_error("SVA: failed to allocate PAL device: %m");
        free(udata->stream_attributes);
        udata->stream_attributes = NULL;
        return -ENOMEM;
    }
    memset(udata->pal_device, 0, sizeof(struct pal_device));
    udata->pal_device->id                      = PAL_DEVICE_IN_HANDSET_VA_MIC;
    udata->pal_device->config.sample_rate      = SVA_SAMPLE_RATE;
    udata->pal_device->config.bit_width        = SVA_BIT_WIDTH;
    udata->pal_device->config.ch_info.channels = SVA_CHANNELS;
    udata->pal_device->config.ch_info.ch_map[0] = PAL_CHMAP_CHANNEL_FL;
    udata->pal_device->config.aud_fmt_id       = PAL_AUDIO_FMT_PCM_S16_LE;

    rc = pal_stream_open(stream_attr, udata->no_of_devices, udata->pal_device,
                        0, NULL,
                        sva_pal_stream_callback, (uint64_t)udata,
                        &udata->sva_pal_handle);
    if (rc != 0 || !udata->sva_pal_handle) {
        pw_log_error("SVA: pal_stream_open FAILED rc=%d", rc);
        return rc;
    }

    /* 3. Build pal_st_phrase_sound_model (KEYPHRASE type) */
    size_t sm_hdr_size = sizeof(struct pal_st_phrase_sound_model);
    pal_param_payload *sound_model_payload = calloc(1,
        sizeof(pal_param_payload) + sm_hdr_size + udata->sva_cfg.model_size);
    if (!sound_model_payload) {
        pal_stream_close(udata->sva_pal_handle);
        udata->sva_pal_handle = NULL;
        return -ENOMEM;
    }
    /* payload_size = content only (not including pal_param_payload header) */
    sound_model_payload->payload_size = (uint32_t)(sm_hdr_size + udata->sva_cfg.model_size);
    struct pal_st_phrase_sound_model *phrase_sm =
        (struct pal_st_phrase_sound_model *)sound_model_payload->payload;
    struct pal_st_sound_model *common_sm = &phrase_sm->common;
    common_sm->type        = PAL_SOUND_MODEL_TYPE_KEYPHRASE;
    common_sm->data_size   = udata->sva_cfg.model_size;
    common_sm->data_offset = (uint32_t)sm_hdr_size;
    /* HeySnapdragon vendor UUID */
    common_sm->vendor_uuid.timeLow          = 0x68ab2d40;
    common_sm->vendor_uuid.timeMid          = 0xe860;
    common_sm->vendor_uuid.timeHiAndVersion = 0x11e3;
    common_sm->vendor_uuid.clockSeq         = 0x95ef;
    common_sm->vendor_uuid.node[0] = 0x00;
    common_sm->vendor_uuid.node[1] = 0x02;
    common_sm->vendor_uuid.node[2] = 0xa5;
    common_sm->vendor_uuid.node[3] = 0xd5;
    common_sm->vendor_uuid.node[4] = 0xc5;
    common_sm->vendor_uuid.node[5] = 0x1b;
    /* Configure the keyphrase entry expected by the sound model. */
    phrase_sm->num_phrases = 1;
    phrase_sm->phrases[0].id               = SVA_PHRASE_ID;
    phrase_sm->phrases[0].recognition_mode = PAL_RECOGNITION_MODE_VOICE_TRIGGER;
    phrase_sm->phrases[0].num_users        = 0;
    /* Copy .uim binary after header */
    memcpy((uint8_t *)phrase_sm + sm_hdr_size,
        udata->sva_cfg.model_data, udata->sva_cfg.model_size);
    pw_log_warn("SVA: LOAD_SOUND_MODEL payload_size=%u sm_hdr=%zu "
                "model=%u phrase_id=%u",
                sound_model_payload->payload_size,
                sm_hdr_size,
                udata->sva_cfg.model_size,
                SVA_PHRASE_ID);
    rc = pal_stream_set_param(udata->sva_pal_handle,
                            PAL_PARAM_ID_LOAD_SOUND_MODEL,
                            (void *)sound_model_payload);
    free(sound_model_payload);
    
    pw_log_warn("SVA: LOAD_SOUND_MODEL rc=%d", rc);
    if (rc != 0) {
        pw_log_error("SVA: LOAD_SOUND_MODEL FAILED rc=%d", rc);
        pal_stream_close(udata->sva_pal_handle);
        udata->sva_pal_handle = NULL;
        return rc;
    }
    pw_log_warn("SVA: Sound model LOADED (KEYPHRASE type, %u bytes)", udata->sva_cfg.model_size);
    return 0;
}


static void sva_emit_props_changed(struct pw_userdata *udata)
{
    const char *state_names[] = { "idle", "loaded", "active", "detected" };
    pw_log_warn("SVA: state=%s model=%s conf=%u mode=%s",
                state_names[udata->sva_state],
                udata->sva_cfg.model_path[0] ? udata->sva_cfg.model_path : "(none)",
                udata->sva_cfg.confidence_level,
                udata->sva_cfg.mode);
}

static void sva_set_state(struct pw_userdata *udata, enum sva_state next_state)
{
    const char *state_names[] = { "IDLE", "LOADED", "ACTIVE", "DETECTED" };
    if (udata->sva_state == next_state && next_state != SVA_STATE_DETECTED)
        return;
    pw_log_warn("SVA: [TRANSITION] %s -> %s",
                state_names[udata->sva_state], state_names[next_state]);
    udata->sva_state = next_state;
    sva_emit_props_changed(udata);
    sva_schedule_snapshot(udata);
}

/*
 * SVA_STATE_DETECTED is a transient edge, not a resting state — it marks
 * "a detection just happened" for the one snapshot a client sees right
 * after it. Once this detection's associated LAB activity (drain and/or
 * delivery) has fully resolved — successfully or not — nothing else will
 * ever move the state machine again until the *next* detection, so without
 * this call sva.detection.status would stay published as "detected"
 * forever and falsely look like a brand-new edge to any later subscriber
 * (including the same client re-fetching via sva.lab.acquire). Call this
 * at every point a detection's LAB cycle concludes; it is a no-op if some
 * unrelated transition (restart/reset/model-reload) already moved the
 * state machine elsewhere in the meantime.
 */
static bool sva_lab_cycle_resolved(struct pw_userdata *udata)
{
    if (udata->sva_state != SVA_STATE_DETECTED)
        return false;

    /*
     * A detection is one-shot as far as PAL is concerned — PAL does not
     * resume listening for the next phrase on its own (same reason
     * sva.restart has to resend RECOGNITION_CONFIG + call
     * pal_stream_start() again, see sva_start_recognition()). Re-arm must
     * happen, and must succeed, before we publish ACTIVE: a client that
     * sees "active" is told it's safe to assume the next wake word will be
     * caught, so publishing ACTIVE ahead of a re-arm that then fails would
     * mislead every subscriber into believing recognition is live when
     * it's not.
     *
     * Safe to do here: every caller of sva_lab_cycle_resolved() only
     * reaches this point once this detection's own LAB drain has fully
     * finished (drain reads from PAL's history buffer; delivery, if any,
     * reads only from our own cache) — so re-arming PAL's history buffer
     * now can never clobber data this cycle still needed to capture.
     */
    if (udata->sva_pal_handle) {
        int rc = sva_set_recognition_config(udata);
        if (rc == 0)
            rc = pal_stream_start(udata->sva_pal_handle);
        if (rc == 0) {
            udata->sva_rt.last_rearm_mono_ms = sva_now_ms();
            pw_log_warn("[SVA] Recognition RE-ARMED after detection — "
                        "listening for the next phrase (PAL's history "
                        "buffer is now empty, same as sva.restart)");
            sva_set_state(udata, SVA_STATE_ACTIVE);
        } else {
            pw_log_error("[SVA] failed to re-arm recognition after "
                         "detection, rc=%d — PAL will NOT report further "
                         "detections until sva.restart is sent", rc);
            sva_set_state(udata, SVA_STATE_LOADED);
        }
    } else {
        pw_log_error("[SVA] cannot re-arm recognition: PAL handle is NULL");
        sva_set_state(udata, SVA_STATE_IDLE);
    }
    return true;
}

/*
 * Clear every piece of per-cycle runtime state: LAB drain/delivery progress,
 * the cached LAB itself, and the last detection's metadata. Shared by
 * close_pal_stream() (disconnect/error/reset path) and sva.restart, so a
 * disconnect, a reset, or a restart can never leave behind state that
 * silently re-arms on the next unrelated reconnect (the "no delivery
 * without a connected client" requirement).
 */
static void sva_reset_runtime_fields(struct pw_userdata *udata)
{
    udata->sva_rt.lab_draining          = false;
    udata->sva_rt.lab_bytes_read        = 0;
    udata->sva_rt.lab_bytes_total       = 0;
    udata->sva_rt.lab_delivery_active   = false;
    udata->sva_rt.lab_delivery_offset   = 0;
    udata->sva_rt.lab_cache_valid       = false;
    udata->sva_rt.detection_confidence  = 0;
    udata->sva_rt.capture_delay_ms      = 0;
    udata->sva_rt.capture_preamble_ms   = 0;
    udata->sva_rt.capture_available     = false;
    udata->sva_rt.lab_event[0]          = '\0';
}

static int close_pal_stream(struct pw_userdata *udata)
{
    int rc = -1;
    if (udata->is_sva) {
        if (udata->sva_pal_handle) {
            pal_stream_stop(udata->sva_pal_handle);
            pal_stream_close(udata->sva_pal_handle);
            udata->sva_pal_handle = NULL;
        }
        sva_reset_runtime_fields(udata);
        return 0;
    }

    if (udata->stream_handle) {
        pw_log_error("close_pal_stream enter: handle=%p offload=%d pal_ready=%d wait_logged=%d",
                udata->stream_handle, udata->is_offload, udata->pal_ready,
                udata->pal_wait_logged);
        udata->pal_ready = false;
        udata->pal_wait_logged = false;

        if (udata->isplayback && !udata->stream_ready) {
            /* starter thread still running inside pal_stream_start.
             * Signal it to skip volume and exit quickly, then
             * pal_stream_stop below will unblock it. */
            udata->stop_requested = true;
            __sync_synchronize();
            pw_log_info("playback: stop requested while starter thread running");
        }
        if (!udata->isplayback && udata->reader_running) {
            udata->reader_running = false;
            pthread_join(udata->reader_thread, NULL);
            free(udata->ring_buf);
            udata->ring_buf = NULL;
            pthread_mutex_destroy(&udata->ring_mutex);
            pthread_cond_destroy(&udata->ring_cond);
            pw_log_info("capture reader thread stopped");
        }
        rc = pal_stream_stop(udata->stream_handle);
        if (rc) {
            pw_log_error("pal_stream_stop failed for %p error %d", udata->stream_handle, rc);
        }
        rc = pal_stream_close(udata->stream_handle);
        if (rc)
            pw_log_error("could not close sink handle %p, error %d", udata->stream_handle, rc);
        udata->stream_handle = NULL;
    }
    else
        return 0;

    return rc;
}
static void pw_pal_stream_start(struct pw_userdata *udata)
{
    int rc = 0;
    pal_buffer_config_t out_buf_cfg, in_buf_cfg;
    udata->pal_ready = false;
    udata->pal_wait_logged = false;

    rc = pal_stream_open(udata->stream_attributes, udata->no_of_devices, udata->pal_device,
         0, NULL, pa_pal_out_cb, (uint64_t)udata, &udata->stream_handle);

    if (rc) {
        udata->stream_handle = NULL;
        pw_log_error("Could not open output stream %d", rc);
        goto exit;
    }

    if (udata->isplayback) {
        in_buf_cfg.buf_size = 0;
        in_buf_cfg.buf_count = 0;
        out_buf_cfg.buf_size = udata->sink_buf_size;
        out_buf_cfg.buf_count = udata->sink_buf_count;
    } else {
        out_buf_cfg.buf_size = 0;
        out_buf_cfg.buf_count = 0;
        in_buf_cfg.buf_size = udata->source_buf_size;
        in_buf_cfg.buf_count = udata->source_buf_count;
    }

    rc = pal_stream_set_buffer_size(udata->stream_handle, &in_buf_cfg, &out_buf_cfg);
    if(rc) {
        pw_log_error("pal_stream_set_buffer_size failed\n");
        goto cleanup;
    }
    if (!udata->isplayback) {
        /* capture: start synchronously then launch reader thread */
        rc = pal_stream_start(udata->stream_handle);
        if (rc) {
            pw_log_error("pal_stream_start failed, error %d\n", rc);
            goto cleanup;
        }
    } else {
        /* playback: start asynchronously so RT thread is never blocked.
         * process() will send silence until stream_ready becomes true. */
        udata->stream_ready   = false;
        udata->stop_requested = false;
        __sync_synchronize();
        pthread_create(&udata->starter_thread, NULL,
                       pw_pal_starter_thread, udata);
        /* detach: thread cleans itself up, we never join on main thread */
        pthread_detach(udata->starter_thread);
        pw_log_info("playback starter thread launched (detached)");
    }

    if (!udata->isplayback) {
        /* allocate ring buffer: 8x PAL buffer size, rounded to power-of-two */
        uint32_t ring_sz = udata->source_buf_size * 8;
        /* round up to next power of two */
        ring_sz--;
        ring_sz |= ring_sz >> 1;
        ring_sz |= ring_sz >> 2;
        ring_sz |= ring_sz >> 4;
        ring_sz |= ring_sz >> 8;
        ring_sz |= ring_sz >> 16;
        ring_sz++;
        udata->ring_buf   = calloc(1, ring_sz);
        udata->ring_size  = ring_sz;
        udata->ring_write = 0;
        udata->ring_read  = 0;
        pthread_mutex_init(&udata->ring_mutex, NULL);
        pthread_cond_init(&udata->ring_cond, NULL);
        udata->reader_running = true;
        pthread_create(&udata->reader_thread, NULL, pw_pal_reader_thread, udata);
        pw_log_info("capture reader thread launched, ring_size=%u", ring_sz);
    }
    if (!udata->is_offload)
        udata->pal_ready = true;
    else
        pw_log_info("PAL compressed stream started; waiting for write-ready callback");

    return;
cleanup:
    if (close_pal_stream(udata))
        pw_log_error("could not close sink handle %p", udata->stream_handle);
exit:
    return;

}
static void pw_pal_change_stream_state(void *d, enum pw_stream_state old,
        enum pw_stream_state state, const char *error)
{
    struct pw_userdata *udata = d;
    
    switch (state) {
    case PW_STREAM_STATE_ERROR:
    case PW_STREAM_STATE_UNCONNECTED:
        if (!udata->is_sva) {
              pw_impl_module_schedule_destroy(udata->module);
        } else {
              pw_log_error("[SVA] stream unconnected/error — releasing PAL resources");
              close_pal_stream(udata);
              sva_set_state(udata, SVA_STATE_IDLE);
        }
        break;
    case PW_STREAM_STATE_PAUSED:
        if (!udata->is_sva) {
            close_pal_stream(udata);
        } else if (udata->sva_rt.lab_delivery_active) {
            /* Consumer disconnected mid-delivery. Stop feeding a data path
             * nobody is reading. The cache itself is left valid (untouched)
             * so the next client to connect — or an explicit
             * sva.lab.acquire — can still receive it; status recomputes to
             * "buffered" (or "idle" if the cache was somehow already gone)
             * via sva_lab_status_name(), never left claiming "delivering".
             * Also resolve this detection's cycle (re-arming PAL recognition)
             * if it's still pending — otherwise a disconnect that happens to
             * land mid-delivery would leave sva_state stuck on DETECTED and
             * PAL never re-armed, identical in effect to the "stuck" bug a
             * normal delivery-complete path already avoids. */
            udata->sva_rt.lab_delivery_active = false;
            udata->sva_rt.lab_delivery_offset = 0;
            pw_log_warn("[SVA] consumer disconnected mid-delivery — delivery cancelled, cache preserved");
            sva_lab_cycle_resolved(udata);
            sva_schedule_snapshot(udata);
        }
        break;
    case PW_STREAM_STATE_STREAMING:
        if (!udata->is_sva) {
            pw_pal_stream_start(udata);
        } else {
            /* This fires once, for whichever client causes our node to
             * first start — it will NOT fire again for a later client that
             * links in while we're already streaming (e.g. pw-voiceui kept
             * open, then pw-record started afterward). Install the
             * peer_added listener here (idempotent) so that later-arriving
             * recorder peers get caught by sva_node_peer_added() instead of
             * being silently missed. */
            sva_ensure_peer_listener(udata);
            /* A client connecting (or already connected when a detection
             * completed) is itself the implicit request for cached LAB —
             * this is what lets pw-record, which has no SVA prop
             * vocabulary, receive LAB the same way pw-voiceui does. Try to
             * auto-start delivery from a buffered cache before publishing,
             * so the very first snapshot this client sees already reflects
             * "delivering" rather than a stale "buffered". Skip this when
             * the only connected peer is a pw-voiceui `listen` session
             * (media.role=VoiceUI) — that session is a persistent
             * notification listener, not an implicit requester; it must
             * use the explicit `replay` command (sva.lab.acquire) instead.
             * See sva_has_recorder_peer(). */
            if (sva_has_recorder_peer(udata))
                sva_lab_try_auto_start(udata);
            /* sva_set_state()/sva_emit_props_changed() only publish on an
             * actual state transition — a client that connects while the
             * plugin has already settled into its current state (no
             * transition pending) would otherwise see nothing and report
             * "state: unknown" until some unrelated future transition
             * happens to fire. Publish the current snapshot unconditionally
             * so every client sees accurate state the moment it starts
             * streaming, regardless of transition timing. */
            sva_schedule_snapshot(udata);
        }
        break;
    default:
        break;
    }
}

static void pw_pal_process_stream(void *d)
{
    struct pw_userdata *udata = d;
    struct pw_buffer *buf = pw_stream_dequeue_buffer(udata->stream);
    struct spa_data *bd;
    struct pal_buffer pal_buf = { 0 };
    uint32_t size = 0;
    bool need_publish = false;
    bool lab_event_set = false;
    int rc;

    if (!buf) { pw_log_error("out of buffers: %m"); return; }
    if (!buf->buffer || buf->buffer->n_datas == 0) goto queue;
    bd = &buf->buffer->datas[0];
    if (!bd->chunk || !bd->data) goto queue;

    if (udata->isplayback) {
        uint32_t offs = SPA_MIN(bd->chunk->offset, bd->maxsize);
        size = SPA_MIN(bd->chunk->size, bd->maxsize - offs);
        void *data = SPA_PTROFF(bd->data, offs, void);

        /* wait until pal_stream_start has returned in the starter thread */
        if (!udata->stream_ready) {
            /* stream not ready yet - drop this buffer silently */
            pw_stream_queue_buffer(udata->stream, buf);
            return;
        }

        pal_buf.buffer = data;
        pal_buf.size = size;

        if (udata->stream_handle && size > 0) {
            if (udata->is_offload && !udata->pal_ready) {
                if (!udata->pal_wait_logged) {
                    pw_log_info("PAL compressed stream has no write-ready callback yet; attempting initial write");
                    udata->pal_wait_logged = true;
                }
            }

            rc = pal_stream_write(udata->stream_handle, &pal_buf);
            if (udata->is_offload)
                pw_log_info("PAL compressed write returned: rc=%d, requested bytes=%u, pal_ready=%d", rc, size, udata->pal_ready);

            if (rc < 0) {
                pw_log_error("pal_stream_write failed: %d", rc);
                if (udata->is_offload) {
                    udata->pal_ready = false;
                    udata->pal_wait_logged = false;
                }
                close_pal_stream(udata);
            } else {
                size_t written = (size_t)rc;
                buf->size = written / udata->frame_size;
            }
        }
    } else if (udata->is_sva) {
        bool drain_just_completed = false;
        size = buf->requested ? SPA_MIN(buf->requested * udata->frame_size, bd->maxsize) : bd->maxsize;
        pal_buf.buffer = bd->data;
        pal_buf.size = size;
        if (udata->sva_rt.lab_draining && udata->sva_pal_handle) {
            pw_log_trace("[SVA] PipeWire process entered");
            /*
             * Drain PAL's LAB buffer straight into our own cache (not
             * bd->data) — this keeps the background drain running so
             * PAL's internal buffer doesn't stall. "size" stays 0 the
             * whole time we're draining; once the drain completes below,
             * sva_lab_try_auto_start() may begin delivering this same
             * cache to a connected client, but never within this same
             * quantum's bd->data.
             */
            {
                uint32_t remaining = udata->sva_rt.lab_bytes_total -
                                     udata->sva_rt.lab_bytes_read;
                pal_buf.size = SPA_MIN(pal_buf.size, remaining);
            }
            if (udata->sva_rt.lab_cache_data &&
                udata->sva_rt.lab_bytes_read + pal_buf.size <= udata->sva_rt.lab_cache_capacity) {
                pal_buf.buffer = udata->sva_rt.lab_cache_data + udata->sva_rt.lab_bytes_read;
            } else {
                pal_buf.buffer = bd->data; /* cache full/unavailable: drain and discard */
            }
            rc = pal_stream_read(udata->sva_pal_handle, &pal_buf);
            size = 0; /* never forwarded onto the live data path this quantum */
            if (rc < 0) {
                pw_log_error("[SVA] LAB read failed: %d", rc);
                udata->sva_rt.lab_draining = false;
                snprintf(udata->sva_rt.lab_event, sizeof(udata->sva_rt.lab_event), "error");
                if (sva_lab_cycle_resolved(udata)) {
                    /* cycle_resolved's own publish already showed+cleared
                     * the "error" event; one more corrects the node's
                     * cached snapshot to the now-steady state. */
                    sva_schedule_snapshot(udata);
                } else {
                    lab_event_set = true;
                    need_publish = true;
                }
            } else if (rc == 0) {
                udata->sva_rt.lab_draining = false;
                if (udata->sva_rt.lab_bytes_read > 0 && udata->sva_rt.lab_cache_data) {
                    /* PAL may still legitimately run dry before
                     * lab_bytes_total is reached (e.g. detection fired
                     * before a full lab_duration of history had
                     * accumulated). The bytes already drained are real,
                     * valid PCM — cache them as a shorter-than-requested
                     * but complete capture instead of discarding them.
                     *
                     * Most common cause: a restart/reset wipes PAL's
                     * history ring buffer to empty (see
                     * last_rearm_mono_ms); if detection fires less than
                     * lab_duration ms after that, there is physically
                     * not enough elapsed real audio to satisfy the
                     * full request — this is expected, not data loss. */
                    uint64_t elapsed = sva_now_ms() - udata->sva_rt.last_rearm_mono_ms;
                    pw_log_warn("[SVA] LAB read returned zero bytes after %u/%u "
                                "— less history was available than the "
                                "requested LAB duration; caching partial capture "
                                "(%llu ms elapsed since last start/restart/reset vs "
                                "%u ms requested — detection fired before the full "
                                "history window had accumulated)",
                                udata->sva_rt.lab_bytes_read, udata->sva_rt.lab_bytes_total,
                                (unsigned long long)elapsed, udata->sva_cfg.lab_duration);
                    udata->sva_rt.lab_cache_size = udata->sva_rt.lab_bytes_read;
                    udata->sva_rt.lab_cache_valid = true;
                    udata->sva_rt.lab_cache_generation++;
                    pw_log_warn("[SVA] LAB cache populated: %u bytes (generation=%u)",
                                udata->sva_rt.lab_cache_size,
                                udata->sva_rt.lab_cache_generation);
                    drain_just_completed = true;
                } else {
                    pw_log_warn("[SVA] LAB read returned zero bytes; stopping drain");
                    snprintf(udata->sva_rt.lab_event, sizeof(udata->sva_rt.lab_event), "error");
                    if (sva_lab_cycle_resolved(udata)) {
                        sva_schedule_snapshot(udata);
                    } else {
                        lab_event_set = true;
                        need_publish = true;
                    }
                }
            } else {
                uint32_t got = (uint32_t)rc;
                udata->sva_rt.lab_bytes_read += got;
                if (udata->sva_rt.lab_bytes_read >= udata->sva_rt.lab_bytes_total)
                    udata->sva_rt.lab_draining = false;
                pw_log_trace("[SVA] LAB progress=%u/%u", udata->sva_rt.lab_bytes_read, udata->sva_rt.lab_bytes_total);
                if (!udata->sva_rt.lab_draining) {
                    pw_log_info("[SVA] LAB complete=%u", udata->sva_rt.lab_bytes_read);
                    if (udata->sva_rt.lab_cache_data) {
                        udata->sva_rt.lab_cache_size = udata->sva_rt.lab_bytes_read;
                        udata->sva_rt.lab_cache_valid = true;
                        udata->sva_rt.lab_cache_generation++;
                        pw_log_info("[SVA] LAB cache populated: %u bytes (generation=%u)",
                                    udata->sva_rt.lab_cache_size,
                                    udata->sva_rt.lab_cache_generation);
                    }
                    drain_just_completed = true;
                }
            }
        } else if (udata->sva_rt.lab_delivery_active && udata->sva_rt.lab_cache_valid) {
            uint32_t remaining = udata->sva_rt.lab_cache_size -
                                 udata->sva_rt.lab_delivery_offset;
            uint32_t copy_size = SPA_MIN(pal_buf.size, remaining);
            memcpy(bd->data,
                   udata->sva_rt.lab_cache_data + udata->sva_rt.lab_delivery_offset,
                   copy_size);
            udata->sva_rt.lab_delivery_offset += copy_size;
            size = copy_size;
            pw_log_trace("[SVA] LAB delivery progress=%u/%u",
                        udata->sva_rt.lab_delivery_offset, udata->sva_rt.lab_cache_size);
            if (udata->sva_rt.lab_delivery_offset >= udata->sva_rt.lab_cache_size) {
                udata->sva_rt.lab_delivery_active = false;
                snprintf(udata->sva_rt.lab_event, sizeof(udata->sva_rt.lab_event), "complete");
                pw_log_info("[SVA] LAB delivery complete=%u", udata->sva_rt.lab_delivery_offset);
                if (sva_lab_cycle_resolved(udata)) {
                    /* cycle_resolved's own publish already showed+cleared
                     * the "complete" event; one more corrects the node's
                     * cached snapshot to the now-steady state. */
                    sva_schedule_snapshot(udata);
                } else {
                    lab_event_set = true;
                    need_publish = true;
                }
            }
        } else size = 0;

        if (drain_just_completed) {
            /* Hand the fresh cache to whatever client is already
             * connected and streaming, the moment it's ready — the
             * "already listening" auto-delivery trigger. The "connects
             * afterward" trigger lives in
             * pw_pal_change_stream_state()'s STREAMING case. Skip when
             * the only connected peer is a pw-voiceui `listen` session
             * (see sva_has_recorder_peer()) — it must explicitly ask via
             * `replay` instead. In that case this detection's LAB cycle
             * is already fully resolved (the cache sits "buffered",
             * nothing further will run for it), so revert
             * SVA_STATE_DETECTED now the same way the "never armed a
             * drain" path does — resolve the detection cycle so the
             * state does not remain DETECTED, since no delivery will
             * ever complete to resolve it. */
            if (sva_has_recorder_peer(udata)) {
                sva_lab_try_auto_start(udata);
                need_publish = true;
            } else if (!sva_lab_cycle_resolved(udata)) {
                /* cycle_resolved() returning true means it already
                 * published the resolved steady state itself — avoid
                 * publishing the same steady-state snapshot twice. Only
                 * publish here if it was a no-op. */
                need_publish = true;
            }
        }

        bd->chunk->offset = 0;
        bd->chunk->size = size;
        bd->chunk->stride = udata->frame_size;
        buf->size = udata->frame_size ? size / udata->frame_size : 0;
    } else {
        uint32_t quantum_size = buf->requested ? buf->requested * udata->frame_size : bd->maxsize;
        uint32_t avail, to_copy;
        void *data = bd->data;

        /* Copy from ring buffer filled by the reader thread (non-blocking) */
        pthread_mutex_lock(&udata->ring_mutex);
        avail = (udata->ring_write - udata->ring_read) & (udata->ring_size - 1);
        to_copy = SPA_MIN(avail, quantum_size);
        if (to_copy > 0) {
            uint32_t r = udata->ring_read & (udata->ring_size - 1);
            uint32_t tail = udata->ring_size - r;
            if (to_copy <= tail) {
                memcpy(data, udata->ring_buf + r, to_copy);
            } else {
                memcpy(data, udata->ring_buf + r, tail);
                memcpy((uint8_t *)data + tail, udata->ring_buf, to_copy - tail);
            }
            udata->ring_read += to_copy;
        }
        pthread_mutex_unlock(&udata->ring_mutex);

        if (to_copy < quantum_size)
            memset((uint8_t *)data + to_copy, 0, quantum_size - to_copy);

        bd->chunk->size   = quantum_size;
        bd->chunk->stride = udata->frame_size;
        bd->chunk->offset = 0;
        buf->size = quantum_size / udata->frame_size;
    }
queue:
    rc = pw_stream_queue_buffer(udata->stream, buf);
    if (rc < 0) {
        pw_log_error("pw_stream_queue_buffer failed: %s", spa_strerror(rc));
    } else if (need_publish) {
        /* lab_event (if set) is a one-shot transient — the first publish
         * shows it to live subscribers and clears it as a side effect
         * (see sva_publish_snapshot_on_loop()); publish a second time
         * right after so the node's cached Props value a later subscriber
         * would see is the corrected steady state, never the transient. */
        sva_schedule_snapshot(udata);
        if (lab_event_set)
            sva_schedule_snapshot(udata);
    }
}

static int sva_start_recognition(struct pw_userdata *udata)
{
    int rc = sva_load_sound_model(udata);
    if (rc != 0) return rc;
    sva_set_state(udata, SVA_STATE_LOADED);
    rc = sva_set_recognition_config(udata);
    if (rc != 0) { close_pal_stream(udata); sva_set_state(udata, SVA_STATE_IDLE); return rc; }
    rc = pal_stream_start(udata->sva_pal_handle);
    if (rc != 0) { close_pal_stream(udata); sva_set_state(udata, SVA_STATE_IDLE); return rc; }
    udata->sva_rt.last_rearm_mono_ms = sva_now_ms();
    sva_set_state(udata, SVA_STATE_ACTIVE);
    pw_log_warn("[SVA] Recognition STARTED");
    return 0;
}

static void pw_pal_change_stream_param(void *data, uint32_t id, const struct spa_pod *param) {
    struct pw_userdata *udata = data;

    if (param == NULL)
        return;

        if (udata->is_sva && id == SPA_PARAM_Props) {
        struct spa_pod_object *obj = (struct spa_pod_object *)param;
        struct spa_pod_prop *prop;
        bool model_path_updated = false;
        SPA_POD_OBJECT_FOREACH(obj, prop) {
            if (prop->key == SPA_PROP_params) {
                const struct spa_pod *iter;
                const struct spa_pod *params_struct = &prop->value;
                if (!spa_pod_is_struct(params_struct))
                    continue;
                SPA_POD_STRUCT_FOREACH(params_struct, iter) {
                    const char *key;
                    if (spa_pod_get_string(iter, &key) < 0)
                        break;
                    const struct spa_pod *value = spa_pod_next(iter);
                    if (!value)
                        break;
                    const char *str_val;
                    int32_t int_val;
                    if (spa_streq(key, "sva.sound.model.path") || spa_streq(key, "sva.model.path")) {
                        if (spa_pod_get_string(value, &str_val) == 0) {
                            int n = snprintf(udata->sva_cfg.model_path,
                                              sizeof(udata->sva_cfg.model_path), "%s", str_val);
                            if (n < 0 || (size_t)n >= sizeof(udata->sva_cfg.model_path)) {
                                pw_log_error("SVA: model path truncated (len=%d, max=%zu) — "
                                             "rejecting, not loading a wrong/partial path: %s",
                                             n, sizeof(udata->sva_cfg.model_path) - 1, str_val);
                                udata->sva_cfg.model_path[0] = '\0';
                                return;
                            }
                            model_path_updated = true;
                        }
                    } else if (spa_streq(key, "sva.confidence.level")) {
                        if (spa_pod_get_int(value, &int_val) == 0 && int_val >= 0 && int_val <= 100)
                            udata->sva_cfg.confidence_level = (uint32_t)int_val;
                        else if (spa_pod_get_string(value, &str_val) == 0) {
                            char *end; long v = strtol(str_val, &end, 10);
                            if (*str_val && *end == '\0' && v >= 0 && v <= 100)
                                udata->sva_cfg.confidence_level = (uint32_t)v;
                            else { pw_log_error("SVA: invalid confidence level"); return; }
                        }
                    } else if (spa_streq(key, "sva.lab.duration")) {
                        /* hist_buffer_duration_msec is sent to PAL as the real
                         * ask (see sva_set_recognition_config()) — PAL sizes
                         * its ring buffer dynamically from whatever is
                         * requested here, there is no fixed hardware ceiling
                         * to clamp against. Accept any positive value so a
                         * client requesting N ms always gets N ms back. */
                        if (spa_pod_get_int(value, &int_val) == 0 && int_val > 0) {
                            udata->sva_cfg.lab_duration = (uint32_t)int_val;
                        } else { pw_log_error("SVA: invalid LAB duration"); return; }
                    } else if (spa_streq(key, "sva.lab.enabled")) {
                        bool bool_val;
                        if (spa_pod_get_bool(value, &bool_val) == 0)
                            udata->sva_cfg.lab_enabled = bool_val;
                    } else if (spa_streq(key, "sva.mode")) {
                        if (spa_pod_get_string(value, &str_val) == 0)
                            snprintf(udata->sva_cfg.mode, sizeof(udata->sva_cfg.mode), "%s", str_val);
                    } else if (spa_streq(key, "sva.reset")) {
                       bool val = false;
                       if (spa_pod_get_bool(value, &val) == 0 && val) {
                          pw_log_warn("SVA: sva.reset requested — releasing PAL resources "
                                      "(gap since last detection=%llu ms, lab_draining=%s)",
                                      (unsigned long long)(sva_now_ms() - udata->sva_rt.last_detect_mono_ms),
                                      udata->sva_rt.lab_draining ? "true" : "false");
                          if (udata->sva_rt.lab_draining)
                              pw_log_warn("SVA: sva.reset racing an in-progress LAB drain");
                          close_pal_stream(udata);
                          udata->sva_cfg.model_path[0] = '\0';
                          sva_set_state(udata, SVA_STATE_IDLE);
                          pw_log_warn("SVA: sva.reset complete — PAL's history buffer "
                                      "is released; the next sva.sound.model.path reload "
                                      "starts it empty, same elapsed-time floor on LAB "
                                      "duration as sva.restart (see sva_start_recognition)");
                          return;
                        }
                   } else if (spa_streq(key, "sva.restart")) {
                       bool val = false;
                       if (spa_pod_get_bool(value, &val) == 0 && val) {
                          pw_log_warn("SVA: sva.restart requested "
                                      "(gap since last detection=%llu ms, lab_draining=%s)",
                                      (unsigned long long)(sva_now_ms() - udata->sva_rt.last_detect_mono_ms),
                                      udata->sva_rt.lab_draining ? "true" : "false");
                          if (udata->sva_rt.lab_draining)
                              pw_log_warn("SVA: sva.restart racing an in-progress LAB drain");
                          if (udata->sva_pal_handle &&
                              (udata->sva_state == SVA_STATE_DETECTED ||
                               udata->sva_state == SVA_STATE_ACTIVE)) {
                              pw_log_warn("SVA: sva.restart requested — restarting "
                                          "recognition, model stays loaded");
                              sva_reset_runtime_fields(udata);
                              /* pal_stream_start() alone does not re-arm
                               * RECOGNITION_CONFIG — PAL falls back to its own
                               * internal default history-buffer duration,
                               * silently truncating every post-restart LAB
                               * capture regardless of the configured
                               * sva.lab.duration. Resend it so the restarted
                               * cycle is armed with the currently configured
                               * duration, same as the initial start. */
                              int rc = sva_set_recognition_config(udata);
                              if (rc == 0)
                                  rc = pal_stream_start(udata->sva_pal_handle);
                              if (rc == 0) {
                                  udata->sva_rt.last_rearm_mono_ms = sva_now_ms();
                                  sva_set_state(udata, SVA_STATE_ACTIVE);
                                  pw_log_warn("SVA: Recognition RESTARTED — "
                                              "LAB/detection runtime state cleared. "
                                              "PAL's history buffer is now EMPTY — a "
                                              "detection in the next %u ms will yield "
                                              "less than the configured %u ms of LAB, "
                                              "since that much real audio hasn't been "
                                              "captured yet (not a bug, see lab_bytes_read "
                                              "vs lab_bytes_total on a short capture)",
                                              udata->sva_cfg.lab_duration,
                                              udata->sva_cfg.lab_duration);
                              } else {
                                  pw_log_error("SVA: sva.restart FAILED rc=%d", rc);
                              }
                          } else {
                              pw_log_warn("SVA: sva.restart requested but no model "
                                          "loaded (state=%d) — ignoring", udata->sva_state);
                          }
                          return;
                        }
                   } else if (spa_streq(key, "sva.lab.acquire")) {
                       /* Internal-only prop: a generic client like pw-record
                        * never sends this (it has no SVA prop vocabulary) and
                        * still gets served via sva_lab_try_auto_start()'s two
                        * implicit triggers (drain-complete, STREAMING). This
                        * is purely a manual re-fetch path for pw-voiceui. */
                       bool val = false;
                       if (spa_pod_get_bool(value, &val) == 0 && val) {
                          if (sva_lab_try_auto_start(udata)) {
                              pw_log_warn("SVA: sva.lab.acquire requested — delivering "
                                          "cached LAB (%u bytes, generation=%u)",
                                          udata->sva_rt.lab_cache_size,
                                          udata->sva_rt.lab_cache_generation);
                              sva_schedule_snapshot(udata);
                          } else if (!udata->sva_rt.lab_cache_valid) {
                              pw_log_warn("SVA: sva.lab.acquire requested but no cached "
                                          "LAB is available — ignoring");
                              /* steady state is already "idle" — no event needed */
                              sva_schedule_snapshot(udata);
                          } else {
                              pw_log_warn("SVA: sva.lab.acquire requested while a drain "
                                          "or delivery is already in progress — ignoring, "
                                          "client should retry shortly");
                              snprintf(udata->sva_rt.lab_event, sizeof(udata->sva_rt.lab_event), "busy");
                              sva_schedule_snapshot(udata);
                          }
                          return;
                        }
                   }
                    iter = value;
                }
            }
        }
        pw_log_warn("SVA: Props received via pw_stream param_changed");
        pw_log_warn("SVA:   model.path = %s", udata->sva_cfg.model_path[0] ? udata->sva_cfg.model_path : "(not set)");
        pw_log_warn("SVA:   confidence = %u", udata->sva_cfg.confidence_level);
        /* Trigger model load if IDLE and path is set; if a new model path
         * arrives while a previous session is LOADED/ACTIVE/DETECTED,
         * release it first instead of silently ignoring the request. */
        if (model_path_updated && udata->sva_cfg.model_path[0] != '\0' &&
            udata->sva_state != SVA_STATE_IDLE) {
            pw_log_warn("SVA: New model path received while active (state=%d) — "
                        "releasing previous session before reload", udata->sva_state);
            close_pal_stream(udata);
            sva_set_state(udata, SVA_STATE_IDLE);
        }
        if (model_path_updated && udata->sva_state == SVA_STATE_IDLE &&
            udata->sva_cfg.model_path[0] != '\0') {
            int rc = sva_start_recognition(udata);
            if (rc != 0)
                pw_log_error("SVA: recognition start failed rc=%d", rc);
        }
        return;
    }
    if (id == SPA_PARAM_Format)
    {
        if (spa_format_parse(param, &udata->format.media_type, &udata->format.media_subtype) < 0)
            return;
        if (udata->format.media_type == SPA_MEDIA_TYPE_audio &&
            udata->format.media_subtype == SPA_MEDIA_SUBTYPE_raw) {
            spa_format_audio_raw_parse(param, &udata->format.info.raw);
        } else {
            pw_log_info("Compressed format detected: subtype=%u", udata->format.media_subtype);
        }
    }    
}

static const struct pw_stream_events pw_pal_stream_events = {
    PW_VERSION_STREAM_EVENTS,
    .destroy = pw_pal_destroy_stream,
    .state_changed = pw_pal_change_stream_state,
    .process = pw_pal_process_stream,
    .param_changed = pw_pal_change_stream_param
};

static int pw_pal_create_stream(struct pw_userdata *udata)
{
    int res;
    uint32_t n_params = 0;
    const struct spa_pod *params[2];
    uint8_t buffer[1024];
    struct spa_pod_builder b;
    spa_pod_builder_init(&b, buffer, sizeof(buffer));
    if (udata->isplayback) {
        udata->stream = pw_stream_new(udata->core, "example sink", udata->stream_props);
        if (udata->is_offload) {
            params[n_params++] = spa_pod_builder_add_object(&b,
                            SPA_TYPE_OBJECT_ParamBuffers, SPA_PARAM_Buffers,
                            SPA_PARAM_BUFFERS_buffers, SPA_POD_Int(udata->sink_buf_count),
                            SPA_PARAM_BUFFERS_blocks,  SPA_POD_Int(1),
                            SPA_PARAM_BUFFERS_size,    SPA_POD_Int(udata->sink_buf_size),
                            SPA_PARAM_BUFFERS_stride,  SPA_POD_Int(udata->frame_size));
        } else {
            params[n_params++] = spa_pod_builder_add_object(&b,
                            SPA_TYPE_OBJECT_ParamBuffers, SPA_PARAM_Buffers,
                            SPA_PARAM_BUFFERS_buffers, SPA_POD_Int(udata->sink_buf_count),
                            SPA_PARAM_BUFFERS_blocks,  0,
                            SPA_PARAM_BUFFERS_size,    SPA_POD_Int(udata->sink_buf_size),
                            SPA_PARAM_BUFFERS_stride,  SPA_POD_Int(udata->frame_size));
        }
    } else {
        udata->stream = pw_stream_new(udata->core, "example source", udata->stream_props);
        params[n_params++] = spa_pod_builder_add_object(&b,
                        SPA_TYPE_OBJECT_ParamBuffers, SPA_PARAM_Buffers,
                        SPA_PARAM_BUFFERS_buffers, SPA_POD_Int(udata->source_buf_count),
                        SPA_PARAM_BUFFERS_blocks,  0,
                        SPA_PARAM_BUFFERS_size,    SPA_POD_Int(udata->source_buf_size),
                        SPA_PARAM_BUFFERS_stride,  SPA_POD_Int(udata->frame_size));
    }

    if (udata->stream == NULL)
        return -errno;

    pw_stream_add_listener(udata->stream,
            &udata->stream_listener,
            &pw_pal_stream_events, udata);

    if (udata->is_offload) {
        struct spa_audio_info codec_info;

        spa_zero(codec_info);
        codec_info.media_type = SPA_MEDIA_TYPE_audio;
        codec_info.media_subtype = udata->codec_spa_subtype;

        if (udata->codec_spa_subtype == SPA_MEDIA_SUBTYPE_mp3) {
            codec_info.info.mp3.rate = udata->codec_sample_rate;
            codec_info.info.mp3.channels = udata->codec_channels;
            codec_info.info.mp3.channel_mode =
                udata->codec_channels == 1 ?
                SPA_AUDIO_MP3_CHANNEL_MODE_MONO :
                SPA_AUDIO_MP3_CHANNEL_MODE_STEREO;
        } else if (udata->codec_spa_subtype == SPA_MEDIA_SUBTYPE_aac) {
            codec_info.info.aac.rate = udata->codec_sample_rate;
            codec_info.info.aac.channels = udata->codec_channels;
            codec_info.info.aac.bitrate = udata->codec_bit_rate;
            codec_info.info.aac.stream_format = SPA_AUDIO_AAC_STREAM_FORMAT_MP4ADTS;
        }

        params[n_params++] = spa_format_audio_build(&b,
                        SPA_PARAM_EnumFormat, &codec_info);
    } else {
        params[n_params++] = spa_format_audio_raw_build(&b,
                        SPA_PARAM_EnumFormat, &udata->info);
    }

    res = pw_stream_connect(udata->stream,
              udata->isplayback ? PW_DIRECTION_INPUT : PW_DIRECTION_OUTPUT,
              PW_ID_ANY,
              PW_STREAM_FLAG_AUTOCONNECT |
              PW_STREAM_FLAG_NO_CONVERT |
              PW_STREAM_FLAG_MAP_BUFFERS |
              (udata->is_sva ? 0 : PW_STREAM_FLAG_RT_PROCESS),
              params, n_params);
    if (res < 0) {
        pw_log_error("pw_stream_connect failed: %s", spa_strerror(res));
        return res;
    }
    if (udata->is_offload)
        pw_stream_set_active(udata->stream, true);
    return 0;
}

static void pw_pal_core_error(void *data, uint32_t id, int seq, int res, const char *message)
{
    struct pw_userdata *udata = data;

    pw_log_error("error id:%u seq:%d res:%d (%s): %s",
            id, seq, res, spa_strerror(res), message);

    if (id == PW_ID_CORE && res == -EPIPE)
        pw_impl_module_schedule_destroy(udata->module);
}

static const struct pw_core_events pw_pal_events_core = {
    PW_VERSION_CORE_EVENTS,
    .error = pw_pal_core_error,
};

static void pw_pal_core_destroy(void *d)
{
    struct pw_userdata *udata = d; 
    spa_hook_remove(&udata->core_listener);
    udata->core = NULL;
    pw_impl_module_schedule_destroy(udata->module);
}

static const struct pw_proxy_events pw_pal_proxy_events_core = {
    .destroy = pw_pal_core_destroy,
};

static void pw_pal_userdata_destroy(struct pw_userdata *udata)
{
    if (udata->sva_node_listener_added) {
        spa_hook_remove(&udata->sva_node_listener);
        udata->sva_node_listener_added = false;
    }
    close_pal_stream(udata);
    if (udata->sva_cfg.model_data) {
        free(udata->sva_cfg.model_data);
        udata->sva_cfg.model_data = NULL;
    }
    if (udata->sva_rt.lab_cache_data) {
        free(udata->sva_rt.lab_cache_data);
        udata->sva_rt.lab_cache_data = NULL;
    }

    if (udata->stream)
        pw_stream_destroy(udata->stream);
    if (udata->pal_device) {
        free(udata->pal_device);
        udata->pal_device = NULL;
    }
    if (udata->stream_attributes) {
        free(udata->stream_attributes);
        udata->stream_attributes = NULL;
    }
    if (fcntl(udata->jack_fd, F_GETFD) != -1 || errno != EBADF)
        close(udata->jack_fd);
    if (udata->core && udata->do_disconnect)
        pw_core_disconnect(udata->core);
    pw_properties_free(udata->stream_props);
    pw_properties_free(udata->props);
    free(udata);
}

static void pw_pal_module_destroy(void *data)
{
    struct pw_userdata *udata = data;
    spa_hook_remove(&udata->module_listener);
    pw_pal_userdata_destroy(udata);
}

static const struct pw_impl_module_events pw_pal_events_module = {
    PW_VERSION_IMPL_MODULE_EVENTS,
    .destroy = pw_pal_module_destroy,
};

static inline uint32_t format_from_name(const char *name, size_t len)
{
    int i;
    for (i = 0; spa_type_audio_format[i].name; i++) {
        if (strncmp(name, spa_debug_type_short_name(spa_type_audio_format[i].name), len) == 0)
            return spa_type_audio_format[i].type;
    }
    return SPA_AUDIO_FORMAT_UNKNOWN;
}

static uint32_t pw_pal_get_channel(const char *name)
{
    int i;
    for (i = 0; spa_type_audio_channel[i].name; i++) {
        if (spa_streq(name, spa_debug_type_short_name(spa_type_audio_channel[i].name)))
            return spa_type_audio_channel[i].type;
    }
    return SPA_AUDIO_CHANNEL_UNKNOWN;
}

static void pw_pal_get_parse_position(struct spa_audio_info_raw *info, const char *val, size_t len)
{
    struct spa_json it[2];
    char v[256];

    spa_json_init(&it[0], val, len);
        if (spa_json_enter_array(&it[0], &it[1]) <= 0)
                spa_json_init(&it[1], val, len);

    info->channels = 0;
    while (spa_json_get_string(&it[1], v, sizeof(v)) > 0 &&
        info->channels < SPA_AUDIO_MAX_CHANNELS) {
        info->position[info->channels++] = pw_pal_get_channel(v);
    }
}

static void pw_pal_fetch_audio_info(const struct pw_properties *props, struct spa_audio_info_raw *info)
{
    const char *str;

    spa_zero(*info);

    if ((str = pw_properties_get(props, PW_KEY_AUDIO_FORMAT)) == NULL)
        str = PW_DEFAULT_SAMPLE_FORMAT;
    info->format = format_from_name(str, strlen(str));

    info->rate = pw_properties_get_uint32(props, PW_KEY_AUDIO_RATE, info->rate);
    if (info->rate == 0)
        info->rate = PW_DEFAULT_SAMPLE_RATE;

    info->channels = pw_properties_get_uint32(props, PW_KEY_AUDIO_CHANNELS, info->channels);
    info->channels = SPA_MIN(info->channels, SPA_AUDIO_MAX_CHANNELS);
    if ((str = pw_properties_get(props, SPA_KEY_AUDIO_POSITION)) != NULL)
        pw_pal_get_parse_position(info, str, strlen(str));
    if (info->channels == 0)
        pw_pal_get_parse_position(info, PW_DEFAULT_SAMPLE_POSITION, strlen(PW_DEFAULT_SAMPLE_POSITION));
}

static int pw_pal_get_frame_size(const struct spa_audio_info_raw *audio_info)
{
    int res = audio_info->channels;
    switch (audio_info->format) {
    case SPA_AUDIO_FORMAT_U8:
    case SPA_AUDIO_FORMAT_S8:
    case SPA_AUDIO_FORMAT_ALAW:
    case SPA_AUDIO_FORMAT_ULAW:
        return res;
    case SPA_AUDIO_FORMAT_S16:
    case SPA_AUDIO_FORMAT_S16_OE:
    case SPA_AUDIO_FORMAT_U16:
        return res * 2;
    case SPA_AUDIO_FORMAT_S24:
    case SPA_AUDIO_FORMAT_S24_OE:
    case SPA_AUDIO_FORMAT_U24:
        return res * 3;
    case SPA_AUDIO_FORMAT_S24_32:
    case SPA_AUDIO_FORMAT_S24_32_OE:
    case SPA_AUDIO_FORMAT_S32:
    case SPA_AUDIO_FORMAT_S32_OE:
    case SPA_AUDIO_FORMAT_U32:
    case SPA_AUDIO_FORMAT_U32_OE:
    case SPA_AUDIO_FORMAT_F32:
    case SPA_AUDIO_FORMAT_F32_OE:
        return res * 4;
    case SPA_AUDIO_FORMAT_F64:
    case SPA_AUDIO_FORMAT_F64_OE:
        return res * 8;
    default:
        return 0;
    }
}

static void pw_pal_set_props(struct pw_userdata *udata, struct pw_properties *props, const char *key)
{
    const char *str;
    if ((str = pw_properties_get(props, key)) != NULL) {
        if (pw_properties_get(udata->stream_props, key) == NULL)
            pw_properties_set(udata->stream_props, key, str);
    }
}

static size_t pw_stream_get_buffer_size(struct pw_userdata *udata, struct pal_media_config spec, pal_stream_type_t type)
{
        uint32_t buffer_duration = PW_DEFAULT_BUFFER_DURATION_MS;
        size_t length = 0, frames = 0;
        switch (type) {
        case PAL_STREAM_DEEP_BUFFER:
            buffer_duration = PW_DEEP_BUFFER_BUFFER_DURATION_MS;
            break;
        case PAL_STREAM_LOW_LATENCY:
            buffer_duration = PW_LOW_LATENCY_BUFFER_DURATION_MS;
        default:
            break;
        }

        frames = spec.sample_rate * buffer_duration;
        length = ((frames * udata->frame_size) / 1000);

        return (length/udata->frame_size) * udata->frame_size;
}
static int pw_pal_fill_stream_info(struct pw_userdata *udata)
{
    udata->stream_attributes = calloc(1, sizeof(struct pal_stream_attributes));
    if (!udata->stream_attributes) { pw_log_error("failed to allocate PAL stream attributes"); return -ENOMEM; }
    udata->stream_attributes->type  = udata->stream_type;
    udata->stream_attributes->flags = 0;
    if (!udata->is_sva) {
        udata->stream_attributes->info.opt_stream_info.version      = 1;
        udata->stream_attributes->info.opt_stream_info.duration_us  = -1;
        udata->stream_attributes->info.opt_stream_info.has_video    = false;
        udata->stream_attributes->info.opt_stream_info.is_streaming = false;
    } else {
        udata->stream_attributes->info.voice_rec_info.version = 1;
    }

    if (udata->isplayback) {
        udata->stream_attributes->direction = PAL_AUDIO_OUTPUT;
        udata->stream_attributes->out_media_config.bit_width = 16;
        udata->stream_attributes->out_media_config.ch_info.ch_map[0] = PAL_CHMAP_CHANNEL_FL;
        udata->stream_attributes->out_media_config.ch_info.ch_map[1] = PAL_CHMAP_CHANNEL_FR;
        udata->sink_buf_count = 4;
        if(!(udata->is_offload)) {
            udata->stream_attributes->out_media_config.sample_rate = udata->info.rate;
            switch (udata->stream_attributes->out_media_config.bit_width) {
                case 32: udata->stream_attributes->out_media_config.aud_fmt_id = PAL_AUDIO_FMT_PCM_S32_LE; break;
                case 24: udata->stream_attributes->out_media_config.aud_fmt_id = PAL_AUDIO_FMT_PCM_S24_3LE; break;
                default: udata->stream_attributes->out_media_config.aud_fmt_id = PAL_AUDIO_FMT_DEFAULT_PCM; break;
            }
            udata->stream_attributes->out_media_config.ch_info.channels = udata->info.channels;
            udata->sink_buf_size = pw_stream_get_buffer_size(udata, udata->stream_attributes->out_media_config, udata->stream_type);
        } else {
            udata->stream_attributes->flags  = PAL_STREAM_FLAG_NON_BLOCKING_MASK;
            udata->stream_attributes->out_media_config.sample_rate      = udata->codec_sample_rate;
            udata->stream_attributes->out_media_config.ch_info.channels = udata->codec_channels;
            udata->stream_attributes->out_media_config.aud_fmt_id       = udata->codec_aud_fmt_id;
            udata->sink_buf_size = 16484;
        }
    } else {
        udata->stream_attributes->direction = PAL_AUDIO_INPUT;
        if (!udata->is_sva) {
            udata->stream_attributes->in_media_config.sample_rate = udata->info.rate;
            udata->stream_attributes->in_media_config.bit_width = 16;
            switch (udata->stream_attributes->in_media_config.bit_width) {
                case 32: udata->stream_attributes->in_media_config.aud_fmt_id = PAL_AUDIO_FMT_PCM_S32_LE; break;
                case 24: udata->stream_attributes->in_media_config.aud_fmt_id = PAL_AUDIO_FMT_PCM_S24_3LE; break;
                default: udata->stream_attributes->in_media_config.aud_fmt_id = PAL_AUDIO_FMT_DEFAULT_PCM; break;
            }
            udata->stream_attributes->in_media_config.ch_info.channels = udata->info.channels;
            udata->stream_attributes->in_media_config.ch_info.ch_map[0] = PAL_CHMAP_CHANNEL_FL;
            udata->stream_attributes->in_media_config.ch_info.ch_map[1] = PAL_CHMAP_CHANNEL_FR;
            if (udata->info.channels >= 3)
                udata->stream_attributes->in_media_config.ch_info.ch_map[2] = PAL_CHMAP_CHANNEL_C;
            if (udata->info.channels >= 4) {
                udata->stream_attributes->in_media_config.ch_info.ch_map[2] = PAL_CHMAP_CHANNEL_LB;
                udata->stream_attributes->in_media_config.ch_info.ch_map[3] = PAL_CHMAP_CHANNEL_RB;
            }

            udata->source_buf_size = PW_DEFAULT_CAPTURE_QUANTUM_FRAMES * udata->frame_size;
            udata->source_buf_count = 4;
        } else {
            udata->stream_attributes->in_media_config.sample_rate  = udata->sva_cfg.sample_rate;
            udata->stream_attributes->in_media_config.bit_width    = SVA_BIT_WIDTH;
            udata->stream_attributes->in_media_config.aud_fmt_id   = PAL_AUDIO_FMT_PCM_S16_LE;
            udata->stream_attributes->in_media_config.ch_info.channels   = udata->sva_cfg.channels;
            udata->stream_attributes->in_media_config.ch_info.ch_map[0]  = PAL_CHMAP_CHANNEL_FL;
            udata->source_buf_size  = SVA_SOURCE_BUF_SIZE;
            udata->source_buf_count = SVA_SOURCE_BUF_COUNT;
        }
    }

    if (udata->pal_device) free(udata->pal_device);
    udata->pal_device = calloc(udata->no_of_devices, sizeof(struct pal_device));
    if (!udata->pal_device) {
    pw_log_error("failed to allocate PAL devices");
    free(udata->stream_attributes);
    udata->stream_attributes = NULL;
    return -ENOMEM;
}

    for (int i = 0; i < udata->no_of_devices; i++) {
        udata->pal_device[i].id = udata->pal_device_id[i];
        
        if (!udata->is_sva) {
            udata->pal_device[i].config.sample_rate = 48000;
            udata->pal_device[i].config.bit_width = 16;
            udata->pal_device[i].config.ch_info.channels = 2;
            udata->pal_device[i].config.ch_info.ch_map[0] = PAL_CHMAP_CHANNEL_FL;
            udata->pal_device[i].config.ch_info.ch_map[1] = PAL_CHMAP_CHANNEL_FR;
            udata->pal_device[i].config.aud_fmt_id = PAL_AUDIO_FMT_PCM_S16_LE;
        } else {
            udata->pal_device[i].config.sample_rate = SVA_SAMPLE_RATE;
            udata->pal_device[i].config.bit_width = SVA_BIT_WIDTH;
            udata->pal_device[i].config.ch_info.channels = SVA_CHANNELS;
            udata->pal_device[i].config.ch_info.ch_map[0] = PAL_CHMAP_CHANNEL_FL;
            udata->pal_device[i].config.aud_fmt_id = PAL_AUDIO_FMT_PCM_S16_LE;
        }
    } return 0;
}

static inline bool pw_stream_is_running(struct pw_userdata *udata)
{
    if (udata == NULL || udata->stream == NULL)
        return false;

    const char *err = NULL;
    enum pw_stream_state st = pw_stream_get_state(udata->stream, &err);

    if (st == PW_STREAM_STATE_ERROR) {
        pw_log_error("%s: stream is in ERROR state%s%s", __func__, err ? ": " : "", err ? err : "");
        return false;
    }

    return st == PW_STREAM_STATE_STREAMING;
}

static int handle_device_connection(struct pw_userdata *udata, bool state)
{
    int ret = 0;
    struct pal_device dev;
    if (!udata) return -EINVAL;

    if (udata->no_of_devices != 1) {
        pw_log_info("%s: combined playback selected, skip routing for jack '%s'",
            __func__, udata->jack_name);
        return 0;
    }

    pw_log_info("%s: processing device connection for jack '%s'", __func__, udata->jack_name);

    if (strstr(udata->jack_name, "DP")) {
        pal_param_device_connection_t *device_connection = (pal_param_device_connection_t *)
            calloc(1, sizeof(pal_param_device_connection_t));
        if (!device_connection) {
            pw_log_error("%s: calloc failed for device_connection", __func__);
            return -ENOMEM;
        }
        device_connection->connection_state = state;
        device_connection->id = udata->pal_device_id[0];
        ret = pal_set_param (PAL_PARAM_ID_DEVICE_CONNECTION, device_connection,
                sizeof(pal_param_device_connection_t));
        free(device_connection);
        return ret;
    }
    else if (strstr(udata->jack_name, "Headset")) {

        if (!pw_stream_is_running(udata) || !udata->stream_handle) {
            pw_log_error("%s: stream not streaming; skip headset routing", __func__);
            return 0;
        }

        const pal_device_id_t target = udata->isplayback
            ? (state ? PAL_DEVICE_OUT_WIRED_HEADSET : PAL_DEVICE_OUT_SPEAKER)
            : (state ? PAL_DEVICE_IN_WIRED_HEADSET  : PAL_DEVICE_IN_SPEAKER_MIC);

        memset(&dev, 0, sizeof(dev));
        dev.id = target;

        ret = pal_stream_set_device(udata->stream_handle, 1, &dev);
        if (ret) {
            pw_log_error("%s: pal_stream_set_device(%d) failed: %d", __func__, target, ret);
            return ret;
        }
        return 0;
    }
    return 0;
}


static void handle_jack_boot_event(struct pw_userdata *udata)
{
    int fd = udata->jack_fd;
    int connected = 0;
    uint8_t sw_bitmask[NUM_BYTES];

    memset(sw_bitmask, 0, sizeof(sw_bitmask));
    // Query current switch state
    if (ioctl(fd, EVIOCGSW(sizeof(sw_bitmask)), sw_bitmask) >= 0) {
        // Check for HDMI/DP jack state
        if (BIT_VALUE(SW_LINEOUT_INSERT, sw_bitmask))
            connected = 1;

        if (connected) {
            pw_log_info("%s: Connected (boot time)", udata->jack_name);
            if(handle_device_connection(udata, true))
                pw_log_error("Failed to handle device connection");
        }
    } else {
        pw_log_error("Failed to query initial jack state");
    }
}

static void on_jack_event(void *userdata, int fd, uint32_t mask)
{
    struct pw_userdata *udata = userdata;
    char name[256] = {0,};
    struct input_event ev;

    ioctl(fd, EVIOCGNAME(sizeof(name)), name);
    if (!strstr(name, udata->jack_name))
        return;

    if (mask & (SPA_IO_ERR | SPA_IO_HUP)) {
        pw_log_error("error or hang-up on the hdmi/dp fd");
        pw_loop_destroy_source(pw_context_get_main_loop(udata->context), udata->jack_src);
        udata->jack_src = NULL;

        if (udata->jack_fd >= 0) {
            close(udata->jack_fd);
            udata->jack_fd = -1;
        }
        return;
    }

    ssize_t ret = read(fd, &ev, sizeof(ev));

    if (ret == sizeof(ev)) {
        if (ev.type != EV_SW)
            return;

        if (ev.code == SW_LINEOUT_INSERT || ev.code == SW_HEADPHONE_INSERT) {
            const char *state = ev.value ? "Connected" : "Disconnected";
            pw_log_info("Jack (%s): %s", udata->jack_name, state);

            if (handle_device_connection(udata, ev.value ? true : false))
                pw_log_error("Failed to handle %s device connection",udata->jack_name);
        }
    } else if (ret < 0) {
        pw_log_error("Error reading event: %s", strerror(errno));
    } else {
        pw_log_error("Short read: got %zd bytes", ret);
    }
}

static int jack_open_fd(struct pw_userdata *udata)
{
    int ret = 0;
    DIR *d = opendir(DEV_INPUT_DIR);
    if (d == NULL) {
        pw_log_error("opendir() failed");
        ret = -ENOENT;
        goto exit;
    }

    struct dirent *dir;
    while ((dir = readdir(d)) != NULL) {
        /* Filter out non block devices and files that don't have
           the right prefix. */
        if (dir->d_type != DT_CHR ||
                strncmp(FILE_PREFIX, dir->d_name,
                    strlen(FILE_PREFIX)) != 0)
            continue;

        char filepath[512];
        snprintf(filepath, sizeof(filepath), "%s/%s",
            DEV_INPUT_DIR, dir->d_name);
        int fd = open(filepath, O_RDONLY);
        if (fd < 0) {
            pw_log_error("open() failed %s", filepath);
            continue;
        }

        char name[256] = {0,};
        ioctl(fd, EVIOCGNAME(sizeof(name)), name);

        /* Search for the keyword in the input's name. */
        if (strstr(name, udata->jack_name) != NULL) {
            closedir(d);
            return fd;
        }

        close(fd);
    }

    closedir(d);
    ret = -EINVAL;
exit:
    pw_log_error("No jack input device found in %s", DEV_INPUT_DIR);
    return ret;
}

static int jack_register(struct pw_userdata *udata)
{
    int ret = 0;
    udata->jack_fd = jack_open_fd(udata);
    if (udata->jack_fd < 0) {
        ret = -EBADFD;
        goto exit;
    }

    ret = fcntl(udata->jack_fd, F_SETFL, O_NONBLOCK);
    if (ret < 0) {
        pw_log_error("fcntl() failed");
        goto exit;
    }

    udata->jack_src = pw_loop_add_io(pw_context_get_main_loop(udata->context), udata->jack_fd,
            SPA_IO_IN | SPA_IO_ERR | SPA_IO_HUP, true, on_jack_event, udata);

    if (udata->jack_src == NULL) {
        pw_log_error("pw_loop_add_io failed for jack_fd=%d", udata->jack_fd);
        ret = -EIO;
    }

exit:
    return ret;
}

static bool pw_pal_codec_type_to_pal(const char *s, pal_audio_fmt_t *f, uint32_t *t) {
    if (!s) return false;
    if (strcasecmp(s,"mp3")==0)      { *f=PAL_AUDIO_FMT_MP3;      *t=SPA_MEDIA_SUBTYPE_mp3;    return true; }
    if (strcasecmp(s,"aac")==0||strcasecmp(s,"aac_adts")==0) { *f=PAL_AUDIO_FMT_AAC_ADTS; *t=SPA_MEDIA_SUBTYPE_aac; return true; }
    if (strcasecmp(s,"aac_adif")==0) { *f=PAL_AUDIO_FMT_AAC_ADIF; *t=SPA_MEDIA_SUBTYPE_aac;    return true; }
    if (strcasecmp(s,"aac_latm")==0) { *f=PAL_AUDIO_FMT_AAC_LATM; *t=SPA_MEDIA_SUBTYPE_aac;    return true; }
    if (strcasecmp(s,"flac")==0)     { *f=PAL_AUDIO_FMT_FLAC;     *t=SPA_MEDIA_SUBTYPE_flac;   return true; }
    if (strcasecmp(s,"vorbis")==0)   { *f=PAL_AUDIO_FMT_VORBIS;   *t=SPA_MEDIA_SUBTYPE_vorbis; return true; }
    if (strcasecmp(s,"opus")==0)     { *f=PAL_AUDIO_FMT_OPUS;     *t=SPA_MEDIA_SUBTYPE_opus;   return true; }
    if (strcasecmp(s,"wma")==0||strcasecmp(s,"wma_std")==0) { *f=PAL_AUDIO_FMT_WMA_STD; *t=SPA_MEDIA_SUBTYPE_wma; return true; }
    if (strcasecmp(s,"wma_pro")==0)  { *f=PAL_AUDIO_FMT_WMA_PRO;  *t=SPA_MEDIA_SUBTYPE_wma;    return true; }
    return false;
}

static uint32_t pw_pal_get_u32(const struct pw_properties *p, const char *k, uint32_t d) {
    const char *s = pw_properties_get(p, k);
    if (!s) return d;
    char *e; unsigned long v = strtoul(s, &e, 10);
    return (e==s||*e) ? d : (uint32_t)v;
}

SPA_EXPORT
int pipewire__module_init(struct pw_impl_module *module, const char *args)
{
    struct pw_context *context = pw_impl_module_get_context(module);
    struct pw_properties *props = NULL;
    const char *offload = NULL;
    uint32_t id = pw_global_get_id(pw_impl_module_get_global(module));
    uint32_t pid = getpid();
    struct pw_userdata *udata;
    const char *str, *value, *role;
    int res = 0;

    PW_LOG_TOPIC_INIT(log_topic);

    udata = calloc(1, sizeof(struct pw_userdata));
    if (udata == NULL)
        return -errno;
    udata->jack_fd = -1;
    if (args == NULL)
        args = "";

    props = pw_properties_new_string(args);
    if (props == NULL) {
        res = -errno;
        pw_log_error( "can't create properties: %m");
        goto error;
    }
    udata->props = props;

    udata->stream_props = pw_properties_new(NULL, NULL);
    if (udata->stream_props == NULL) {
        res = -errno;
        pw_log_error( "can't create properties: %m");
        goto error;
    }

    udata->module = module;
    udata->context = context;
    res = agm_init();
    if (res) {
        pw_log_error("%s: agm init failed\n", __func__);
        goto error;
    }
    res = pal_init();
    if (res) {
        pw_log_error("%s: pal init failed\n", __func__);
        goto error;
    }
    if (pw_properties_get(props, PW_KEY_NODE_VIRTUAL) == NULL)
        pw_properties_set(props, PW_KEY_NODE_VIRTUAL, "true");

    if (pw_properties_get(props, PW_KEY_MEDIA_CLASS) == NULL)
        pw_properties_set(props, PW_KEY_MEDIA_CLASS, "Audio/Sink");

    value = pw_properties_get(props, PW_KEY_MEDIA_CLASS);
    if (value) {
        if (strstr(value, "Sink")) {
            udata->isplayback = true;
            udata->pal_device_id[0] = PAL_DEVICE_OUT_SPEAKER;
        }
        else {
            udata->isplayback = false;
            if (!udata->is_sva) {
                udata->pal_device_id[0] = PAL_DEVICE_IN_SPEAKER_MIC;
            }
        }
    }

    udata->no_of_devices = 1;
    value = pw_properties_get(props, PW_KEY_NODE_NAME);
    if (value) {
        if (strstr(value, "pal_sink_speaker"))
            udata->pal_device_id[0] = PAL_DEVICE_OUT_SPEAKER;
        else if (strstr(value, "pal_sink_headset"))
            udata->pal_device_id[0] = PAL_DEVICE_OUT_WIRED_HEADSET;
        else if (strstr(value, "pal_source_speaker_mic"))
            udata->pal_device_id[0] = PAL_DEVICE_IN_SPEAKER_MIC;
        else if (strstr(value, "pal_source_headset_mic"))
            udata->pal_device_id[0] = PAL_DEVICE_IN_WIRED_HEADSET;
        else if (strstr(value, "pal_sink_dp_out"))
            udata->pal_device_id[0] = PAL_DEVICE_OUT_AUX_DIGITAL;
        else if (strstr(value, "pal_sink_hdmi_out"))
            udata->pal_device_id[0] = PAL_DEVICE_OUT_HDMI;
        else if (strstr(value, "pal_sink_combined")) {
            udata->pal_device_id[0] = PAL_DEVICE_OUT_WIRED_HEADSET;
            udata->pal_device_id[1] = PAL_DEVICE_OUT_SPEAKER;
            udata->no_of_devices = 2;
        }
        else if (strstr(value, "pal_source_voice_ui")) {
            udata->pal_device_id[0] = PAL_DEVICE_IN_HANDSET_VA_MIC;
            udata->is_sva = true;  
        }
    }

    if (pw_properties_get(props, PW_KEY_MEDIA_ROLE) == NULL) {
        if (!udata->is_sva)
            pw_properties_set(props, PW_KEY_MEDIA_ROLE, "notification");
        else
            pw_properties_set(props, PW_KEY_MEDIA_ROLE, "VoiceUI");
    }

    role = pw_properties_get(props, PW_KEY_MEDIA_ROLE);

    if (udata->is_sva || (role && strstr(role, "VoiceUI"))) {
        udata->stream_type = PAL_STREAM_VOICE_UI;
        udata->is_sva = true;
    } else if (role && udata->isplayback) { 
            if (strstr(role,"music"))
                udata->stream_type = PAL_STREAM_DEEP_BUFFER;
            else
                udata->stream_type = PAL_STREAM_LOW_LATENCY;
    } else {
        udata->stream_type = PAL_STREAM_DEEP_BUFFER;
    }

    if (pw_properties_get(props, PW_KEY_NODE_NAME) == NULL)
        pw_properties_setf(props, PW_KEY_NODE_NAME, "example-sink-%u-%u", pid, id);

    if (pw_properties_get(props, PW_KEY_NODE_DESCRIPTION) == NULL)
        pw_properties_set(props, PW_KEY_NODE_DESCRIPTION,
        pw_properties_get(props, PW_KEY_NODE_NAME));

    if ((str = pw_properties_get(props, "stream.props")) != NULL)
        pw_properties_update_string(udata->stream_props, str, strlen(str));

    offload = pw_properties_get(udata->stream_props, "compress.offload");
    udata->is_offload = offload && strcmp(offload, "true") == 0;

    if ((str = pw_properties_get(props, "jack-name")) != NULL)
        snprintf(udata->jack_name, sizeof(udata->jack_name), "%s", str);

    pw_pal_set_props(udata, props, PW_KEY_AUDIO_RATE);
    pw_pal_set_props(udata, props, PW_KEY_AUDIO_CHANNELS);
    pw_pal_set_props(udata, props, SPA_KEY_AUDIO_POSITION);
    pw_pal_set_props(udata, props, PW_KEY_NODE_NAME);
    pw_pal_set_props(udata, props, PW_KEY_NODE_DESCRIPTION);
    pw_pal_set_props(udata, props, PW_KEY_NODE_GROUP);
    pw_pal_set_props(udata, props, PW_KEY_NODE_LATENCY);
    pw_pal_set_props(udata, props, PW_KEY_NODE_VIRTUAL);
    pw_pal_set_props(udata, props, PW_KEY_MEDIA_CLASS);

    pw_pal_fetch_audio_info(udata->stream_props, &udata->info);
    if (udata->is_sva) {
        
        udata->frame_size    = SVA_FRAME_SIZE_BYTES; 
    } else if (!udata->is_offload) {
        udata->frame_size = pw_pal_get_frame_size(&udata->info);
        if (udata->frame_size == 0) {
            res = -EINVAL;
            pw_log_error("can't parse audio format");
            goto error;
        }
    } else {
        udata->codec_aud_fmt_id=PAL_AUDIO_FMT_MP3; udata->codec_spa_subtype=SPA_MEDIA_SUBTYPE_mp3;
        udata->codec_sample_rate=48000; udata->codec_channels=2; udata->codec_bit_rate=128000;
        const char *cs = pw_properties_get(udata->stream_props, "codec.type");
        if (!pw_pal_codec_type_to_pal(cs, &udata->codec_aud_fmt_id, &udata->codec_spa_subtype))
            pw_log_info("codec.type unknown, defaulting to MP3");
        udata->codec_sample_rate = pw_pal_get_u32(udata->stream_props, "codec.sample_rate", udata->codec_sample_rate);
        udata->codec_channels    = pw_pal_get_u32(udata->stream_props, "codec.channels",    udata->codec_channels);
        udata->codec_bit_rate    = pw_pal_get_u32(udata->stream_props, "codec.bit_rate",    udata->codec_bit_rate);
        pw_log_info("compress offload: codec=%s aud_fmt=0x%x rate=%u ch=%u",
                cs?cs:"mp3", udata->codec_aud_fmt_id, udata->codec_sample_rate, udata->codec_channels);
        udata->stream_type = PAL_STREAM_COMPRESSED;
        udata->frame_size = 16;
        pw_properties_set(udata->stream_props, PW_KEY_MEDIA_CLASS, "Audio/Sink");
        pw_properties_set(udata->stream_props, PW_KEY_AUDIO_FORMAT, "encoded");
        pw_properties_set(udata->stream_props, "audio.coding.format", cs?cs:"mp3");
    }
    udata->core = pw_context_get_object(udata->context, PW_TYPE_INTERFACE_Core);
    if (udata->core == NULL) {
        str = pw_properties_get(props, PW_KEY_REMOTE_NAME);
        udata->core = pw_context_connect(udata->context,
                pw_properties_new(
                    PW_KEY_REMOTE_NAME, str,
                    NULL),
                0);
        udata->do_disconnect = true;
    }

    if (udata->core == NULL) {
        res = -errno;
        pw_log_error("can't connect: %m");
        goto error;
    }

    pw_proxy_add_listener((struct pw_proxy*)udata->core,
            &udata->core_proxy_listener,
            &pw_pal_proxy_events_core, udata);
    pw_core_add_listener(udata->core,
            &udata->core_listener,
            &pw_pal_events_core, udata);

    if (udata->is_sva) {
        pw_log_info("SVA: VoiceUI node initialized");
        udata->sva_state = SVA_STATE_IDLE;
        /* Parse SVA config from module args */
        if ((str = pw_properties_get(props, "sva.sound.model.path")) != NULL)
            snprintf(udata->sva_cfg.model_path, sizeof(udata->sva_cfg.model_path), "%s", str);
        if ((str = pw_properties_get(props, "sva.confidence.level")) != NULL)
            udata->sva_cfg.confidence_level = atoi(str);
        else
            udata->sva_cfg.confidence_level = SVA_PDK_CONFIDENCE_LEVEL_DEFAULT;
        if ((str = pw_properties_get(props, "sva.lab.duration")) != NULL) {
            /* No clamp: PAL sizes its ring buffer dynamically from whatever
             * duration is requested (see sva_set_recognition_config()) — there
             * is no fixed hardware ceiling to enforce here. */
            int v = atoi(str);
            udata->sva_cfg.lab_duration = (v > 0) ? (uint32_t)v : SVA_LAB_DURATION;
        } else
            udata->sva_cfg.lab_duration = SVA_LAB_DURATION;
        if ((str = pw_properties_get(props, "sva.lab.enabled")) != NULL)
            udata->sva_cfg.lab_enabled = spa_streq(str, "true");
        else
            udata->sva_cfg.lab_enabled = true;
        if ((str = pw_properties_get(props, "sva.mode")) != NULL)
            snprintf(udata->sva_cfg.mode, sizeof(udata->sva_cfg.mode), "%s", str);
        else
            snprintf(udata->sva_cfg.mode, sizeof(udata->sva_cfg.mode), "NLPI");
        /* PAL's voice-UI path only accepts this fixed capture format —
         * sva.sample.rate/sva.channels/sva.bit.width are not read. */
        udata->sva_cfg.sample_rate = SVA_SAMPLE_RATE;
        udata->sva_cfg.channels    = SVA_CHANNELS;
        udata->sva_cfg.bit_width   = SVA_BIT_WIDTH;
        pw_log_info("SVA config: model=%s confidence=%u mode=%s sr=%u ch=%u",
                    udata->sva_cfg.model_path,
                    udata->sva_cfg.confidence_level,
                    udata->sva_cfg.mode,
                    udata->sva_cfg.sample_rate,
                    udata->sva_cfg.channels);
        /* Create the SVA PipeWire stream with PAL's fixed capture format. */
        udata->info.format = SPA_AUDIO_FORMAT_S16_LE;
        udata->info.rate = SVA_SAMPLE_RATE;
        udata->info.channels = SVA_CHANNELS;
        udata->info.position[0] = SPA_AUDIO_CHANNEL_MONO;
        udata->frame_size = SVA_FRAME_SIZE_BYTES;
        if ((res = pw_pal_fill_stream_info(udata)) < 0)
            goto error;
        if ((res = pw_pal_create_stream(udata)) < 0)
            goto error;
        pw_impl_module_add_listener(module, &udata->module_listener,
                                    &pw_pal_events_module, udata);
        /* Auto-load model if path provided in module args */
        if (udata->sva_state == SVA_STATE_IDLE && udata->sva_cfg.model_path[0] != '\0') {
            int rc = sva_start_recognition(udata);
            if (rc != 0)
                pw_log_error("SVA: auto recognition start failed rc=%d", rc);
        } else {
            pw_log_warn("SVA: No model path — waiting for runtime trigger via pw-voiceui");
        }
        return 0;   
    }

    if ((res = pw_pal_fill_stream_info(udata)) < 0)
        goto error;
    if ((res = pw_pal_create_stream(udata)) < 0)
        goto error;
    pw_impl_module_add_listener(module, &udata->module_listener, &pw_pal_events_module, udata);
    if (udata->jack_name && udata->jack_name[0] != '\0') {
        if(jack_register(udata))
            pw_log_error("failed to register jack event for %s", udata->jack_name);
        else {
            handle_jack_boot_event(udata);
        }
    }
    return 0;

error:
    pw_pal_userdata_destroy(udata);
    return res;
}
