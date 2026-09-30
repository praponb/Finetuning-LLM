WITH product_summary AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_products, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id
)
SELECT p.launch_date, ps.unique_products, ps.quantity_range
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.unique_products >= 9
ORDER BY launch_date
