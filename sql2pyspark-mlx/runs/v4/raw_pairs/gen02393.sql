SELECT s.opened_date, s.store_name, t.avg_quantity
FROM stores s
INNER JOIN (SELECT store_id, AVG(quantity) AS avg_quantity
    FROM inventory
    WHERE quantity BETWEEN 121 AND 220
    GROUP BY store_id) t ON s.id = t.store_id;
