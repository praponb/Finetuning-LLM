WITH product_summary AS (
    SELECT product_id, MIN(quantity) min_quantity, MIN(last_restock) first_last_restock
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    p.launch_date,
    ps.min_quantity,
    ps.first_last_restock
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.min_quantity > 210
ORDER BY p.launch_date
