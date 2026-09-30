SELECT
    s.id,
    s.region,
    s.store_name,
    t.min_last_restock,
    t.quantity_range
FROM stores s
INNER JOIN (SELECT store_id, MIN(last_restock) AS min_last_restock, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE store_id = 4
    GROUP BY store_id) t ON s.id = t.store_id
