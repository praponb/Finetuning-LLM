SELECT
    s.sqft,
    s.store_name,
    t.quantity_count,
    t.latest_last_restock
FROM stores s
LEFT JOIN (SELECT store_id, COUNT(quantity) AS quantity_count, MAX(last_restock) AS latest_last_restock
    FROM inventory
    WHERE store_id IN (1, 4, 5, 6)
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY quantity_count DESC
