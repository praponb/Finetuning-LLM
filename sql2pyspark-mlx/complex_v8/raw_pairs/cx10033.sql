WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE is_skipped = TRUE
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(ms_played_bucket - AVG(ms_played_bucket) OVER (PARTITION BY device, is_skipped), 2) AS ms_played_bucket_vs_device_is_skipped_avg,
        MAX(ms_played_bucket) OVER (PARTITION BY TO_DATE(played_at) ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_bucket_max_7
    FROM base_plays
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MAX(ms_played) AS max_play_ms_played,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM ranked s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played_bucket,
        s.source_or_unknown,
        s.ms_played_bucket_vs_device_is_skipped_avg,
        s.max_play_ms_played,
        s.last_played_at,
        s.play_count,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at
    FROM summarized_rows s
    JOIN plays p ON p.track_id = s.track_id AND p.played_at >= TIMESTAMP '2025-01-13 12:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT *, MONTH(play_played_at) AS play_played_at_month
    FROM exploded_detail_plays
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played_bucket,
    source_or_unknown,
    max_play_ms_played,
    last_played_at,
    play_count,
    play_id,
    play_device,
    play_listener_id,
    play_played_at,
    COALESCE(play_played_at_month, 0) AS play_played_at_month_filled
FROM annotated
ORDER BY track_id, id, play_id
LIMIT 10
