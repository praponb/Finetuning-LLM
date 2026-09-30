WITH store_stats AS (
    SELECT store_id, COUNT(DISTINCT quantity) AS unique_stores, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    s.opened_date,
    ss.unique_stores,
    ss.quantity_count
FROM stores s
INNER JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.quantity_count >= 9;
