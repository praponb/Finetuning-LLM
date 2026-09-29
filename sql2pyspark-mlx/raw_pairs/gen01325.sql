WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, attr_value
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        attr_value,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM exploded
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN value = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        COALESCE(k, 'N/A') AS k_filled
    FROM exploded_carts
    WHERE value IS NOT NULL
      AND k IN ('weight_kg', 'utm_source')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY value, k ORDER BY is_welcome5 DESC NULLS FIRST) AS is_welcome5_cume_dist,
        SUM(is_welcome5) OVER (PARTITION BY k) AS k_sum_is_welcome5
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        k,
        value,
        is_welcome5,
        k_sum_is_welcome5,
        DENSE_RANK() OVER w_hist AS is_welcome5_cume_dist_dense_rank,
        MAX(is_welcome5_cume_dist) OVER (PARTITION BY k, value) AS k_max_is_welcome5_cume_dist
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY k ORDER BY is_welcome5_cume_dist)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN k_max_is_welcome5_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END AS k_max_is_welcome5_cume_dist_tier,
    value,
    CASE WHEN k_sum_is_welcome5 >= 373 THEN 'LARGE' ELSE 'SMALL' END AS k_sum_is_welcome5_tier,
    ROUND(SUM(is_welcome5) / NULLIF(COUNT(CASE WHEN value = 'WELCOME5' THEN 1 END), 0), 2) AS is_welcome5_per_welcome5,
    ROUND(AVG(k_max_is_welcome5_cume_dist), 2) AS avg_k_max_is_welcome5_cume_dist,
    COUNT(DISTINCT k) AS distinct_ks,
    COUNT_IF(value = 'WELCOME5') AS welcome5_events,
    COUNT(*) AS cnt,
    COUNT_IF(k = 'weight_kg') AS weight_kg_events
FROM enriched
GROUP BY ROLLUP (CASE WHEN k_max_is_welcome5_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END, value, CASE WHEN k_sum_is_welcome5 >= 373 THEN 'LARGE' ELSE 'SMALL' END)
HAVING is_welcome5_per_welcome5 > 0
ORDER BY avg_k_max_is_welcome5_cume_dist DESC NULLS FIRST;
