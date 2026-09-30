WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS elem
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        promo,
        ARRAY_JOIN(coupon_codes, '|') AS coupon_codes_text
    FROM flattened
    WHERE created_at > TO_TIMESTAMP('2025-03-01 15:00:00')
      AND promo IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        qty - product_id AS qty_minus_product_id,
        TO_DATE(created_at) AS created_at_date,
        COALESCE(promo, 'N/A') AS resolved_promo
    FROM base
    WHERE created_at >= '2025-02-20'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, AVG(qty_minus_product_id) OVER (PARTITION BY promo) AS promo_avg_qty_minus_product_id
    FROM valid
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, p.age
    FROM scored a
    JOIN customers p
        ON a.customer_id = p.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', created_at_date) AS year_start,
    customer_id,
    AVG(promo_avg_qty_minus_product_id) AS avg_promo_avg_qty_minus_product_id,
    ROUND(SUM(qty_minus_product_id) / NULLIF(COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END), 0), 2) AS qty_minus_product_id_per_welcome5,
    MAX(created_at) AS ended_at
FROM enriched
GROUP BY CUBE (DATE_TRUNC('YEAR', created_at_date), customer_id)
ORDER BY year_start DESC NULLS LAST
