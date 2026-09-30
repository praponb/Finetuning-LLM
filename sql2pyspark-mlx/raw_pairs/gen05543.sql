WITH store_summary AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE store_id = 4
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.sqft,
    ss.quantity_range
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY ss.quantity_range DESC, id
LIMIT 10
