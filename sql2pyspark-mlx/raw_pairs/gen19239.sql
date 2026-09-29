WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
),
cleaned AS (
    SELECT
        *,
        YEAR(created_at) AS created_at_year,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes
    FROM flattened
    WHERE created_at >= DATE '2025-03-01'
),
ranked AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, key ORDER BY created_at, id, key) = 1
),
with_windows AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        SUM(created_at_year) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_year_sum_15m,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id, key) AS event_seq,
        MAX(created_at_year) OVER (PARTITION BY customer_id, key) AS customer_id_max_created_at_year
    FROM ranked
),
filtered AS (
    SELECT
        id,
        coupon_codes,
        key,
        created_at_year_sum_15m,
        customer_id_max_created_at_year,
        CAST(event_seq AS DOUBLE) AS event_seq_num,
        COALESCE(key, 'DIRECT') AS key_filled
    FROM with_windows
    WHERE key IS NOT NULL
      AND key NOT IN ('weight_kg', 'utm_source')
),
filtered_carts AS (
    SELECT
        id,
        key,
        created_at_year_sum_15m,
        customer_id_max_created_at_year,
        key_filled,
        SUBSTRING(key, 1, 4) AS key_prefix
    FROM filtered
    WHERE key IS NOT NULL
)
SELECT
    CASE WHEN customer_id_max_created_at_year >= 87 THEN 'LARGE' ELSE 'SMALL' END AS customer_id_max_created_at_year_tier,
    key,
    APPROX_PERCENTILE(created_at_year_sum_15m, 0.95) AS created_at_year_sum_15m_p95,
    ROUND(SUM(created_at_year_sum_15m) / NULLIF(COUNT(CASE WHEN key = 'weight_kg' THEN 1 END), 0), 2) AS created_at_year_sum_15m_per_weight_kg,
    COUNT(CASE WHEN key = 'utm_source' THEN 1 END) AS utm_source_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(created_at_year_sum_15m) FILTER (WHERE key = 'utm_source') AS utm_source_total_created_at_year_sum_15m
FROM filtered_carts
GROUP BY CASE WHEN customer_id_max_created_at_year >= 87 THEN 'LARGE' ELSE 'SMALL' END, key
HAVING distinct_ids > 1;
