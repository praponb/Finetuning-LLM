WITH product_summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity, MAX(quantity) AS max_quantity
    FROM inventory
    WHERE store_id IN (2, 3, 4, 5)
    GROUP BY product_id
)
SELECT p.stock_qty, ps.total_quantity, ps.max_quantity
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.total_quantity > 1300
ORDER BY total_quantity
