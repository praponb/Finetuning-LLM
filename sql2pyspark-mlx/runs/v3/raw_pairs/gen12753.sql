SELECT store_id, product_id, MAX(quantity) AS max_quantity, COUNT(*) AS num_inventory, COUNT(quantity) AS quantity_count FROM inventory GROUP BY store_id, product_id HAVING quantity_count > 2;
