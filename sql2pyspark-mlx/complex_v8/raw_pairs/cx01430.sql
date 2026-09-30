WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE NOT is_skipped
      AND track_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(ms_played, 2) OVER (ORDER BY ms_played, id) AS next2_ms_played,
        COUNT(*) OVER w_track_id AS rows_in_track_id
    FROM base_plays
    WINDOW w_track_id AS (PARTITION BY track_id)
),
stats AS (
    -- Summarise per group
    SELECT
        source,
        CASE WHEN rows_in_track_id >= 13 THEN 'HIGH' WHEN rows_in_track_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_track_id_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(played_at) AS first_played_at,
        ROUND(100 * COUNT(CASE WHEN device IN ('car', 'desktop', 'phone') THEN 1 END) / COUNT(*), 1) AS pct_device_in_car_desktop_phone,
        MIN(ms_played) AS min_ms_played,
        COUNT(*) AS row_count
    FROM windowed
    GROUP BY source, CASE WHEN rows_in_track_id >= 13 THEN 'HIGH' WHEN rows_in_track_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE pct_device_in_car_desktop_phone < (SELECT MAX(pct_device_in_car_desktop_phone) FROM stats)
)
-- Final projection
SELECT
    source,
    rows_in_track_id_tier,
    distinct_ids,
    first_played_at,
    pct_device_in_car_desktop_phone,
    min_ms_played,
    row_count,
    ABS(min_ms_played - 182750) AS min_ms_played_distance,
    DATE_FORMAT(first_played_at, 'MMM yyyy') AS first_played_at_label
FROM qualified
ORDER BY min_ms_played, distinct_ids, source, rows_in_track_id_tier
LIMIT 10
