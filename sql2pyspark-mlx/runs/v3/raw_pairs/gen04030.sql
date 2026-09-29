SELECT
    s.id,
    s.store_name,
    s.region,
    t.min_last_restock,
    t.quantity_range
FROM stores s
INNER JOIN (SELECT store_id, MIN(last_restock) AS min_last_restock, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.quantity_range > 45
ORDER BY s.store_name
