SELECT s.id, s.opened_date, t.max_quantity
FROM stores s
JOIN (SELECT store_id, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY store_id) t ON s.id = t.store_id;
