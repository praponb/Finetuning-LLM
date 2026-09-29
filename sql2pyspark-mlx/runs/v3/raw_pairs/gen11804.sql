WITH store_summary AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.sqft,
    ss.quantity_range
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
ORDER BY sqft DESC
