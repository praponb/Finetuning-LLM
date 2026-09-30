SELECT id, quantity, product_id, store_id FROM inventory WHERE product_id IN (4, 10) AND MONTH(last_restock) IN (2, 4, 5);
