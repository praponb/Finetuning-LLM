WITH product_summary AS (
    SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    WHERE product_id = 12
    GROUP BY product_id
)
SELECT p.category, ps.avg_quantity, ps.stddev_quantity
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.stddev_quantity > 53
