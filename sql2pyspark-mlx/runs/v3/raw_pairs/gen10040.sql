WITH product_stats AS (
    SELECT product_id, COUNT(*) AS row_count, MIN(quantity) AS min_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.cost, ps.row_count, ps.min_quantity
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.row_count >= 4
ORDER BY row_count
