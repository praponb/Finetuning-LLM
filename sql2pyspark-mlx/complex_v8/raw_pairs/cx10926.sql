WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        ABS(ms_played - 176302) AS ms_played_distance,
        MONTH(played_at) AS played_at_month,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE NOT is_skipped
      AND TO_DATE(played_at) < DATE '2025-02-24'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ms_played_distance + ms_played AS ms_played_distance_plus_ms_played,
        QUARTER(played_at) AS played_at_quarter,
        CASE WHEN device IN ('desktop', 'car') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group
    FROM base
    WHERE track_id <= 24
),
rollup_base AS (
    -- Group-level totals
    SELECT
        CASE WHEN ms_played >= 220498 THEN 'large' WHEN ms_played >= 98052 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        device_group,
        SUM(ms_played_distance_plus_ms_played) AS total_ms_played_distance_plus_ms_played,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        SUM(CASE WHEN device = 'speaker' THEN 1 ELSE 0 END) AS n_device_speaker
    FROM derived
    WHERE ms_played_distance_plus_ms_played <= 720000
    GROUP BY CASE WHEN ms_played >= 220498 THEN 'large' WHEN ms_played >= 98052 THEN 'medium' ELSE 'small' END, device_group
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE total_ms_played_distance_plus_ms_played > (SELECT AVG(total_ms_played_distance_plus_ms_played) FROM rollup_base)
)
-- Final projection
SELECT
    ms_played_tier,
    device_group,
    total_ms_played_distance_plus_ms_played,
    distinct_listener_ids,
    distinct_listener_ids - n_device_speaker AS distinct_listener_ids_minus_n_device_speaker
FROM filtered
WHERE total_ms_played_distance_plus_ms_played BETWEEN 1680000 AND 4560000
ORDER BY total_ms_played_distance_plus_ms_played
