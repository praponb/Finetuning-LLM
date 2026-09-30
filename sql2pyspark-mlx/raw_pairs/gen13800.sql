WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS elem
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        CAST(product_id AS DOUBLE) product_id_str,
        COALESCE(promo, 'N/A') promo_filled
    FROM lines
    WHERE created_at > TO_TIMESTAMP('2025-03-01 22:00:00')
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(product_id_str, qty) least_product_id_str_qty
    FROM base
    WHERE promo IS NOT NULL
      AND promo IS DISTINCT FROM 'WELCOME5'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        coupon_codes,
        qty,
        price,
        promo,
        product_id_str,
        promo_filled,
        least_product_id_str_qty,
        DATEDIFF(CURRENT_DATE(), created_at) / 365 years_since_created_at,
        qty - product_id qty_minus_product_id
    FROM valid
    WHERE qty = 4
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END AS qty_tier,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    ROUND(AVG(least_product_id_str_qty), 2) AS avg_least_product_id_str_qty,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    GROUPING_ID() AS grouping_id
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), CASE WHEN qty >= 5 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('DAY', created_at)), ())
ORDER BY span_minutes ASC NULLS LAST;
