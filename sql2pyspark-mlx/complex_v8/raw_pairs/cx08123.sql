WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', device, source) AS device_source_label
    FROM plays
    WHERE is_skipped = TRUE
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        device_source_label,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, track_id ORDER BY ms_played), 3) AS ms_played_cume_dist,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY source, MONTH(played_at))) AS distinct_device_in_source_played_at_month
    FROM cleaned
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM ranked s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
stats AS (
    -- Group-level totals
    SELECT
        device,
        distinct_device_in_source_played_at_month,
        MIN(first_played_at) AS first_first_played_at,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        COUNT(*) AS n_rows,
        SUM(ms_played) FILTER (WHERE device IN ('tv', 'speaker')) AS ms_played_device_in_tv_speaker,
        COUNT(*) FILTER (WHERE device IN ('speaker', 'car')) AS n_device_in_speaker_car
    FROM with_stats
    GROUP BY 1, 2
)
SELECT device, distinct_device_in_source_played_at_month, metric_name, amount
FROM (
    SELECT device, distinct_device_in_source_played_at_month, distinct_track_ids, n_rows, n_device_in_speaker_car
    FROM stats
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (distinct_track_ids, n_rows, n_device_in_speaker_car)
) unp
ORDER BY device, distinct_device_in_source_played_at_month, metric_name
