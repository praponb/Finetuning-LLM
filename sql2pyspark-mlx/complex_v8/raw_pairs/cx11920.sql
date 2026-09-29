WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-06'
      AND device <> 'tv'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        days_since_played_at,
        source_ref,
        device_slug,
        FIRST_VALUE(days_since_played_at) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_played_at,
        SUM(ms_played) OVER (PARTITION BY device ORDER BY days_since_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM cleaned
),
source_stats AS (
    -- Avg days_since_played_at per source
    SELECT source, AVG(days_since_played_at) AS avg_days_since_played_at
    FROM ranked
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_played_at,
        ROUND(s.days_since_played_at - g.avg_days_since_played_at, 2) AS days_since_played_at_minus_avg
    FROM ranked s
    JOIN source_stats g ON g.source = s.source
    WHERE s.days_since_played_at > g.avg_days_since_played_at
),
per_group AS (
    -- Summarise per group
    SELECT
        device,
        track_id,
        listener_id,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        SUM(days_since_played_at) AS total_days_since_played_at
    FROM vs_group
    WHERE track_id >= 2
    GROUP BY device, track_id, listener_id
    HAVING MAX(days_since_played_at) > 245
)
-- Final projection
SELECT
    device,
    track_id,
    listener_id,
    device_list,
    stddev_ms_played,
    total_days_since_played_at,
    CASE WHEN stddev_ms_played >= 220000.0 THEN 'HIGH' WHEN stddev_ms_played >= 140000.0 THEN 'MEDIUM' ELSE 'LOW' END AS stddev_ms_played_tier
FROM per_group
ORDER BY stddev_ms_played DESC NULLS FIRST
