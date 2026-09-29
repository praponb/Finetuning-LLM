SELECT store_id, COUNT(quantity) AS quantity_count FROM inventory GROUP BY store_id HAVING COUNT(quantity) < 2;
