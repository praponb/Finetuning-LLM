SELECT MIN(last_restock) AS first_last_restock, COUNT(quantity) AS quantity_count FROM inventory WHERE store_id IN (1, 2, 4, 6) AND (store_id IN (3, 4) OR product_id IN (3, 10))
