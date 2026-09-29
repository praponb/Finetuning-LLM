WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        CASE WHEN device IN ('car', 'desktop') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        DATE_TRUNC('DAY', played_at) AS played_at_day,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE ms_played > 44439
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(played_at_day) OVER (PARTITION BY listener_id, device ORDER BY played_at_day DESC, id) AS prev_played_at_day,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY listener_id, device ORDER BY played_at_day DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        RANK() OVER (PARTITION BY listener_id, device ORDER BY played_at_day DESC) AS played_at_day_rank,
        DENSE_RANK() OVER w_listener_id_device_by_played AS played_at_day_dense_rank,
        ROUND(PERCENT_RANK() OVER w_listener_id_device_by_played, 3) AS played_at_day_percent_rank
    FROM base
    WINDOW w_listener_id_device_by_played AS (PARTITION BY listener_id, device ORDER BY played_at_day DESC, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(played_at_day) AS played_at_day_year,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        PERCENTILE_APPROX(played_at_day_percent_rank, 0.5) AS median_played_at_day_percent_rank,
        COUNT(*) AS n_rows,
        MAX(played_at) AS last_played_at
    FROM ranked
    WHERE minutes_since_prev_played_at > 2000.0
    GROUP BY 1
    HAVING COUNT(*) >= 3
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('QUARTER', last_played_at) AS last_played_at_quarter
    FROM grouped
)
-- Final projection
SELECT
    played_at_day_year,
    source_list,
    median_played_at_day_percent_rank,
    n_rows,
    last_played_at_quarter,
    DATEDIFF(DATE '2025-12-31', last_played_at) AS days_since_last_played_at,
    CASE WHEN n_rows >= 7 THEN 'large' WHEN n_rows >= 5 THEN 'medium' ELSE 'small' END AS n_rows_tier
FROM tagged
WHERE n_rows <= 10
ORDER BY n_rows DESC, last_played_at_quarter DESC
