SELECT id, last_restock, product_id FROM inventory WHERE store_id IN (4, 5, 6) UNION ALL SELECT id, last_restock, product_id FROM inventory WHERE store_id = 5;
