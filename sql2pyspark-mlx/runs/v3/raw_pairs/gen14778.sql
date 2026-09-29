SELECT store_id, MIN(quantity) AS min_quantity, COUNT(*) AS total_count FROM inventory WHERE store_id IN (1, 3, 6) GROUP BY store_id;
