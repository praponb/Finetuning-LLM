WITH product_summary AS (
    SELECT product_id, AVG(unit_price) AS avg_unit_price
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.price,
    p.product_name,
    ps.avg_unit_price
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
ORDER BY p.product_name
