WITH lines AS (
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
base AS (
    SELECT *, COALESCE(value, 'Unknown') AS value_filled
    FROM lines
    WHERE created_at >= DATE '2025-03-01'
),
base_carts AS (
    SELECT *, HOUR(created_at) AS created_at_hour
    FROM base
    WHERE created_at >= '2023-04-01'
),
valid AS (
    SELECT
        id,
        value,
        value_filled,
        created_at_hour,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label,
        DATEDIFF(CURRENT_DATE(), created_at) / 30 AS months_since_created_at,
        COALESCE(value, 'N/A') AS value_filled_2
    FROM base_carts
    WHERE value IS NOT NULL
),
scored AS (
    SELECT
        id,
        value_filled,
        created_at_hour,
        value_filled_2,
        MIN(created_at_hour) OVER w_running AS value_min_created_at_hour,
        MIN(created_at_hour) OVER w_running AS value_min_created_at_hour_2,
        MIN(months_since_created_at) OVER w_running AS value_min_months_since_created_at,
        RANK() OVER (ORDER BY created_at_hour DESC NULLS FIRST) AS created_at_hour_rank
    FROM valid
    WINDOW w_running AS (PARTITION BY value)
),
totals AS (
    SELECT
        CASE WHEN created_at_hour_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_hour_rank_tier,
        PERCENTILE_APPROX(value_min_months_since_created_at, ARRAY(0.25, 0.5, 0.75)) AS value_min_months_since_created_at_quartiles,
        COUNT(*) AS event_count,
        APPROX_PERCENTILE(created_at_hour_rank, ARRAY(0.25, 0.5, 0.75)) AS created_at_hour_rank_quartiles
    FROM scored
    GROUP BY CASE WHEN created_at_hour_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING event_count > 1
)
SELECT
    created_at_hour_rank_tier,
    value_min_months_since_created_at_quartiles,
    event_count
FROM totals
ORDER BY event_count DESC, created_at_hour_rank_tier DESC;
