WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS line
)
-- Grouped metrics
SELECT
    CASE WHEN product_id >= 11 THEN 'LARGE' ELSE 'SMALL' END AS product_id_tier,
    promo,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    COUNT(*) AS row_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    RANK() OVER (PARTITION BY CASE WHEN product_id >= 11 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT customer_id) DESC) AS distinct_customer_ids_rank
FROM lines
GROUP BY CASE WHEN product_id >= 11 THEN 'LARGE' ELSE 'SMALL' END, promo
ORDER BY distinct_customer_ids NULLS FIRST;
