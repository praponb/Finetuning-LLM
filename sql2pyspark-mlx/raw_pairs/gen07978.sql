WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        entry.product_id,
        entry.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS entry
)
SELECT
    CASE WHEN product_id >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_tier,
    AVG(product_id) AS avg_product_id,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    SUM(CASE WHEN promo = 'VIP' THEN product_id ELSE 0 END) AS vip_product_id,
    COUNT(*) FILTER (WHERE promo = 'SPRING10') AS spring10_rows
FROM flattened
GROUP BY CASE WHEN product_id >= 11 THEN 'PREMIUM' ELSE 'STANDARD' END;
