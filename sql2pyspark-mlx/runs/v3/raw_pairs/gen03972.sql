SELECT id, last_restock, product_id FROM inventory WHERE last_restock < '2024-04-01' UNION ALL SELECT id, last_restock, product_id FROM inventory WHERE product_id IN (4, 5, 7, 9);
