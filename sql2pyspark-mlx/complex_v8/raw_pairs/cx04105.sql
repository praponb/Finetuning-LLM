WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        CASE WHEN source IN ('album', 'search') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE track_id <= 6
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        ROUND(AVG(ms_played) OVER (PARTITION BY device), 2) AS device_avg_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device
    FROM scoped_plays
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        source,
        ms_played_filled,
        source_group,
        min_ms_played_so_far,
        device_avg_ms_played,
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref
    FROM ranked
    WHERE min_ms_played_so_far BETWEEN 604250 AND 2801500
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        source,
        ms_played_filled,
        source_group,
        device_avg_ms_played,
        source_ref,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at,
        ROUND(device_avg_ms_played * 1.07, 2) AS device_avg_ms_played_adj,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM scored
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN source_group IN ('group_b', 'other') THEN 'group_a' WHEN source_group = 'group_a' THEN 'group_b' ELSE 'other' END AS source_group_group,
        ROUND(device_avg_ms_played_adj / NULLIF(days_since_played_at, 0), 3) AS device_avg_ms_played_adj_per_days_since_played_a,
        CASE WHEN device_avg_ms_played_adj >= 481000.0 THEN 'A' WHEN device_avg_ms_played_adj >= 213250.0 THEN 'B' ELSE 'C' END AS device_avg_ms_played_adj_tier
    FROM enriched
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE device_avg_ms_played > (SELECT MIN(device_avg_ms_played) FROM derived)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, days_since_played_at, ms_played_filled
    FROM screened
) src
UNPIVOT (
    amount FOR field IN (days_since_played_at AS days_since_played_at_v, ms_played_filled AS m_ms_played_filled)
) unp
ORDER BY id, field
