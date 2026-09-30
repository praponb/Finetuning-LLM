WITH store_stats AS (
    SELECT store_id, MAX(quantity) AS max_quantity
    FROM inventory
    WHERE quantity <= 92
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.opened_date,
    ss.max_quantity
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
ORDER BY id DESC
