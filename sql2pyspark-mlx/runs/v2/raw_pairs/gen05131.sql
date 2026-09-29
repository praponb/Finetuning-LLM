WITH product_stats AS (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    GROUP BY product_id
)
SELECT p.stock_qty, ps.stddev_quantity, ps.quantity_range
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.quantity_range > 62
