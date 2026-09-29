SELECT
    s.store_name,
    s.sqft,
    t.total_quantity,
    t.quantity_range
FROM stores s
LEFT JOIN (SELECT store_id, SUM(quantity) AS total_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    WHERE YEAR(last_restock) = 2025
    GROUP BY store_id) t ON s.id = t.store_id
