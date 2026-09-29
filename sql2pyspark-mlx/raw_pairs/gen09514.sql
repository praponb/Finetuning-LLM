WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        line.product_id,
        line.qty,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS line
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        qty,
        promo,
        AVG(qty) OVER (PARTITION BY promo) AS promo_avg_qty,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) AS qty_share,
        COUNT(qty) OVER (PARTITION BY customer_id) AS customer_id_count_qty,
        MIN(product_id) OVER (PARTITION BY promo) AS promo_min_product_id
    FROM flattened
    WHERE qty >= 4
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(customer_id_count_qty, 0) AS customer_id_count_qty_filled,
        ROUND(qty, 2) AS qty_rounded
    FROM enriched
    WHERE promo IS NOT NULL
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        qty,
        promo_avg_qty,
        product_id_share,
        qty_share,
        customer_id_count_qty,
        promo_min_product_id,
        customer_id_count_qty_filled,
        qty_rounded,
        CASE WHEN customer_id_count_qty >= 16 THEN 'High' WHEN customer_id_count_qty >= 5 THEN 'Medium' ELSE 'Low' END AS customer_id_count_qty_level,
        customer_id_count_qty_filled - qty_share AS customer_id_count_qty_filled_minus_qty_share,
        COALESCE(promo, 'DIRECT') AS promo_filled
    FROM base
    WHERE qty_rounded BETWEEN 37 AND 62
)
-- Grouped metrics
SELECT
    CASE WHEN customer_id_count_qty >= 16 THEN 'LARGE' ELSE 'SMALL' END AS customer_id_count_qty_tier,
    CASE WHEN qty_share >= 0.67 THEN 'HIGH' ELSE 'LOW' END AS qty_share_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(qty_rounded, 0.9) AS qty_rounded_p90
FROM base_carts
GROUP BY CASE WHEN customer_id_count_qty >= 16 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN qty_share >= 0.67 THEN 'HIGH' ELSE 'LOW' END
ORDER BY qty_rounded_p90 NULLS LAST
