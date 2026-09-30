SELECT store_id, quantity_count FROM (SELECT store_id, COUNT(quantity) AS quantity_count FROM inventory WHERE product_id IN (1, 5) GROUP BY store_id) AS s WHERE quantity_count >= 8;
