WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        WEEKOFYEAR(created_at) AS created_at_weekofyear
    FROM kept_rows
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 19:00:00')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, AVG(created_at_weekofyear) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_weekofyear_avg_30m
    FROM base
    WHERE customer_id = 7
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY code ORDER BY created_at_weekofyear DESC NULLS FIRST) AS created_at_weekofyear_cume_dist,
        SUM(created_at_weekofyear) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_weekofyear_sum_15m,
        MIN(created_at_weekofyear_avg_30m) OVER w_ordered AS created_at_month_min_created_at_weekofyear_avg_30m,
        STDDEV_SAMP(created_at_weekofyear) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_created_at_weekofyear,
        COUNT(created_at_weekofyear) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_weekofyear_count_2h
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY MONTH(created_at), customer_id)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        created_at_weekofyear + created_at_weekofyear_count_2h AS created_at_weekofyear_plus_created_at_weekofyear_count_2h,
        COALESCE(code, 'OTHER') AS code_clean
    FROM enriched
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 17:00:00')
      AND code IS DISTINCT FROM 'SPRING10'
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        created_at_weekofyear_avg_30m * created_at_weekofyear_plus_created_at_weekofyear_count_2h AS created_at_weekofyear_avg_30m_times_created_at_weekofyear_plus_created_at_weekofyear_count_2h,
        CASE WHEN created_at_weekofyear_cume_dist >= 0.7 THEN 'High' WHEN created_at_weekofyear_cume_dist >= 0.27 THEN 'Medium' ELSE 'Low' END AS created_at_weekofyear_cume_dist_band
    FROM valid
    WHERE code IS NOT NULL
      AND created_at > TO_DATE('2025-03-01')
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN created_at_weekofyear_sum_15m >= 430 THEN 'LARGE' ELSE 'SMALL' END AS created_at_weekofyear_sum_15m_tier,
        CASE WHEN created_at_weekofyear_cume_dist >= 0.9 THEN 'LARGE' ELSE 'SMALL' END AS created_at_weekofyear_cume_dist_tier,
        customer_id,
        COUNT_IF(code = 'BUNDLE') AS bundle_events,
        APPROX_PERCENTILE(customer_id_stddev_samp_created_at_weekofyear, ARRAY(0.25, 0.5, 0.75)) AS customer_id_stddev_samp_created_at_weekofyear_quartiles,
        MIN(created_at) AS first_created_at
    FROM valid_carts
    GROUP BY 1, 2, 3
)
SELECT
    created_at_weekofyear_sum_15m_tier,
    created_at_weekofyear_cume_dist_tier,
    customer_id,
    bundle_events
FROM summary
