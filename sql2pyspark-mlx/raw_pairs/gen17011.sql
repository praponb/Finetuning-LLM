WITH lines AS (
    SELECT
        id,
        customer_id,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            EXPLODE_OUTER(items) AS item
        FROM carts
    ) e
)
SELECT
    CASE WHEN price >= 190 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    ROUND(SUM(qty) / NULLIF(COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END), 0), 2) AS qty_per_spring10,
    COUNT(*) FILTER (WHERE promo = 'WELCOME5') AS welcome5_rows,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    ROUND(AVG(qty), 2) AS avg_qty
FROM lines
GROUP BY CASE WHEN price >= 190 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(qty), 2) > 0
