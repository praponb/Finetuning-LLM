WITH recent_plays AS (
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
        CASE WHEN ms_played >= 239955 THEN 'large' WHEN ms_played >= 92277 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(ms_played) OVER (PARTITION BY device) AS device_min_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device,
        SUM(ms_played) OVER (PARTITION BY device) AS device_sum_ms_played,
        MAX(ms_played) OVER (PARTITION BY device) AS device_max_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY played_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played
    FROM recent_plays
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', played_at_month) AS played_at_month_quarter,
        DATEDIFF(DATE '2025-06-30', played_at_month) AS days_since_played_at_month
    FROM with_trends
)
SELECT
    id,
    track_id,
    is_skipped,
    ms_played_tier,
    played_at_month,
    device_min_ms_played,
    distinct_listener_id_in_device,
    device_max_ms_played,
    played_at_month_quarter,
    days_since_played_at_month,
    SUBSTRING(TRIM(ms_played_tier), 1, 4) AS ms_played_tier_prefix
FROM annotated
ORDER BY ms_played_tier
