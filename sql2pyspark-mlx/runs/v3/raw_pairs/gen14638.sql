WITH product_stats AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT p.stock_qty, p.launch_date, ps.avg_quantity
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.avg_quantity >= 4.1
