WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS line
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(promo) AS clean_promo,
        COALESCE(promo, 'NONE') AS promo_clean
    FROM flattened
    WHERE created_at > TO_DATE('2025-03-02')
      AND promo IS NOT NULL
      AND customer_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN product_id >= 8 THEN 'High' WHEN product_id >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_band
    FROM filtered
    WHERE customer_id IS NOT NULL
)
-- Grouped metrics
SELECT
    product_id,
    COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END) AS bundle_count,
    MIN(created_at) AS started_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min
FROM cleaned
GROUP BY product_id
