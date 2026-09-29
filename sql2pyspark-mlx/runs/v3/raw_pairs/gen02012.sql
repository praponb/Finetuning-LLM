SELECT id, quantity, last_restock, product_id FROM inventory WHERE YEAR(last_restock) = 2023 AND store_id IN (2, 4, 5);
