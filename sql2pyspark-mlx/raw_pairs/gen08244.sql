WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        item.qty,
        item.price,
        item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS item
)
SELECT
    promo,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MAX(qty) FILTER (WHERE promo = 'BUNDLE') AS bundle_max_qty,
    PERCENTILE_APPROX(price, 0.5) AS price_p50,
    MAX(created_at) AS last_created_at
FROM exploded
GROUP BY GROUPING SETS ((promo, DATE_TRUNC('DAY', created_at)), (promo))
HAVING MAX(qty) FILTER (WHERE promo = 'BUNDLE') > 0
ORDER BY activity_day
