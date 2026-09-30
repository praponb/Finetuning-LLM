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
        DAYOFWEEK(played_at) AS played_at_dayofweek,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CASE WHEN device IN ('phone', 'speaker') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE is_skipped = TRUE
      AND ms_played <= 333795
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(device, '#', CAST(played_at_dayofweek AS STRING)) AS device_ref,
        CONCAT_WS(': ', source, device) AS source_device_label,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(played_at_dayofweek) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS played_at_dayofweek_avg_3,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY MONTH(played_at))) AS distinct_device_in_played_at_month
    FROM scored
)
-- Subtotals with CUBE
SELECT
    COALESCE(source, 'ALL') AS source,
    distinct_device_in_played_at_month,
    COALESCE(device_group, 'ALL') AS device_group,
    GROUPING_ID() AS gid,
    SUM(days_since_played_at) AS total_days_since_played_at,
    MAX(played_at) AS last_played_at
FROM metrics
GROUP BY CUBE(source, distinct_device_in_played_at_month, device_group)
ORDER BY source NULLS LAST, distinct_device_in_played_at_month NULLS LAST, device_group NULLS LAST
