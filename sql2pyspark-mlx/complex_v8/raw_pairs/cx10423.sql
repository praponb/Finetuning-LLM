WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ABS(ms_played - 192595) AS ms_played_distance,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref
    FROM plays
    WHERE ms_played > 56858
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY ms_played) AS ms_played_dense_rank,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id) AS distinct_listener_id_in_track_id
    FROM base_plays
    WINDOW w_track_id AS (PARTITION BY track_id)
),
with_details AS (
    -- Attach track attributes
    SELECT s.*, t.title AS track_title
    FROM with_windows s
    JOIN tracks t ON t.id = s.track_id
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MIN(ms_played) AS min_play_ms_played,
        COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM with_details s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_distance,
        device_source_label,
        device_ref,
        distinct_listener_id_in_track_id,
        track_title,
        min_play_ms_played,
        play_is_skipped_count
    FROM with_counts
    WHERE ms_played_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    ms_played_distance,
    device_source_label,
    device_ref,
    distinct_listener_id_in_track_id,
    track_title,
    min_play_ms_played,
    play_is_skipped_count,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown
FROM first_rows
ORDER BY source_or_unknown
