WITH store_summary AS (
    SELECT store_id, COUNT(quantity) quantity_count, MIN(last_restock) first_last_restock
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.sqft,
    ss.quantity_count,
    ss.first_last_restock
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.quantity_count > 8;
