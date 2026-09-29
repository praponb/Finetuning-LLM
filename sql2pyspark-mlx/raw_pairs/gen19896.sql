WITH product_summary AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE last_restock > '2023-09-01'
    GROUP BY product_id
)
SELECT p.launch_date, ps.unique_products
FROM products p
LEFT OUTER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.unique_products > 6
