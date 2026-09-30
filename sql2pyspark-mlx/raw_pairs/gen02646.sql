SELECT
    s.id,
    s.opened_date,
    s.sqft,
    t.first_last_restock,
    t.min_quantity
FROM stores s
INNER JOIN (SELECT store_id, MIN(last_restock) AS first_last_restock, MIN(quantity) AS min_quantity
    FROM inventory
    WHERE last_restock <= '2024-06-16'
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.min_quantity > 200
ORDER BY t.min_quantity DESC, id
LIMIT 10;
