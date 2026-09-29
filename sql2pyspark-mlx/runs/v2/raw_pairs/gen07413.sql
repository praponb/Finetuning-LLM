WITH product_summary AS (
    SELECT product_id, MAX(last_restock) AS latest_last_restock, COUNT(DISTINCT quantity) AS unique_stores
    FROM inventory
    WHERE quantity > 201
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    ps.latest_last_restock,
    ps.unique_stores
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
ORDER BY id, launch_date;
