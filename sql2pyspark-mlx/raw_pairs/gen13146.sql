WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, attr_value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        k,
        attr_value,
        EXPLODE(coupon_codes) AS code
    FROM lines
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(code, ' ', k) AS code_k,
        TRIM(code) AS clean_code
    FROM exploded
    WHERE k IS NOT NULL
      AND k != 'weight_kg'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        code,
        clean_code,
        TRIM(k) AS clean_k,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM base
    WHERE TO_DATE(created_at) < '2025-02-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        SUM(days_since_created_at) OVER (PARTITION BY YEAR(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS days_since_created_at_sum_30m
    FROM cleaned
    WHERE code NOT IN ('SPRING10', 'WELCOME5', 'BUNDLE')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN days_since_created_at_sum_30m >= 338 THEN 'LARGE' ELSE 'SMALL' END AS days_since_created_at_sum_30m_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(days_since_created_at_sum_30m, 0.95) AS days_since_created_at_sum_30m_p95,
    APPROX_PERCENTILE(days_since_created_at_sum_30m, ARRAY(0.25, 0.5, 0.75)) AS days_since_created_at_sum_30m_quartiles,
    PERCENTILE_APPROX(days_since_created_at_sum_30m, 0.95) AS days_since_created_at_sum_30m_p95_2
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), CASE WHEN days_since_created_at_sum_30m >= 338 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('DAY', created_at)))
ORDER BY days_since_created_at_sum_30m_p95_2 DESC NULLS FIRST
