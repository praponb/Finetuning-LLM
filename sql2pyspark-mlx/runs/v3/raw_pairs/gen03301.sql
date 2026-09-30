SELECT id, last_restock, product_id FROM inventory WHERE quantity >= 222 UNION ALL SELECT id, last_restock, product_id FROM inventory WHERE product_id IN (1, 2, 8, 10);
