WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        COALESCE(ms_played, 0) AS ms_played_filled,
        CASE WHEN device IN ('phone', 'speaker') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source = 'search'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        TO_DATE(played_at) AS played_at_day,
        ROUND(AVG(ms_played_filled), 2) AS avg_ms_played_filled,
        COUNT(*) FILTER (WHERE device = 'car') AS n_device_car,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        ROUND(SUM(ms_played_filled) / NULLIF(SUM(ms_played), 0), 3) AS ms_played_filled_to_ms_played_ratio,
        ROUND(AVG(ms_played), 2) AS avg_ms_played
    FROM scoped
    GROUP BY track_id, TO_DATE(played_at)
    HAVING AVG(ms_played_filled) > 44500
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        COUNT(CASE WHEN source = 'album' THEN 1 END) AS play_source_count,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        COALESCE(p.play_count, 0) AS play_count
    FROM grouped s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count_2
    FROM plays
    GROUP BY track_id
),
with_stats_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM with_stats s
    LEFT JOIN play_stats_2 p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    track_id,
    played_at_day,
    n_device_car,
    median_ms_played,
    ms_played_filled_to_ms_played_ratio,
    last_played_at,
    play_source_count,
    play_count
FROM with_stats_2
ORDER BY ms_played_filled_to_ms_played_ratio, track_id, played_at_day
LIMIT 10
