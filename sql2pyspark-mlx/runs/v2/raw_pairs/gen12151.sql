WITH product_stats AS (
    SELECT product_id, COUNT(DISTINCT unit_price) AS unique_orders, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.category,
    ps.unique_orders,
    ps.avg_quantity
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
ORDER BY avg_quantity DESC, unique_orders DESC, id
LIMIT 3
