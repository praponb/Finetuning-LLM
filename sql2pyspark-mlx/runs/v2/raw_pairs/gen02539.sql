SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range, AVG(quantity) AS avg_quantity FROM inventory WHERE store_id = 3 AND store_id IN (3, 5, 6) GROUP BY store_id
