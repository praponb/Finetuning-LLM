WITH product_summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    ps.total_quantity,
    ps.quantity_count
FROM products p
JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.quantity_count > 4
