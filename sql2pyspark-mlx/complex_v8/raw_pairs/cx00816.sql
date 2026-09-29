WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE track_id <= 9
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.device_or_unknown,
        t.bpm AS track_bpm,
        t.title AS track_title,
        t.moods AS track_moods
    FROM prepared_plays s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        track_bpm,
        track_title,
        track_moods,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY device_or_unknown, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(CUME_DIST() OVER (ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        RANK() OVER (ORDER BY played_at, id) AS played_at_rank,
        DENSE_RANK() OVER (PARTITION BY device_or_unknown, source ORDER BY played_at, id) AS played_at_dense_rank,
        SUM(ms_played) OVER (PARTITION BY device_or_unknown, source ORDER BY played_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7,
        SUM(track_bpm) OVER (PARTITION BY device, track_id ORDER BY track_bpm DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS track_bpm_sum_4,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY device, track_id ORDER BY track_bpm DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        COUNT(*) OVER w_device AS rows_in_device,
        SIZE(COLLECT_SET(track_id) OVER w_device) AS distinct_track_id_in_device,
        LAST_VALUE(track_bpm) IGNORE NULLS OVER (PARTITION BY device_or_unknown, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_bpm,
        SUM(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM with_details
    WINDOW w_device AS (PARTITION BY device)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    source,
    track_title,
    played_at_rank,
    played_at_dense_rank,
    track_bpm_sum_4,
    rows_in_device,
    last_known_track_bpm,
    running_ms_played
FROM metrics
ORDER BY running_ms_played DESC
