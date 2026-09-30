SELECT id, last_restock, product_id FROM inventory WHERE last_restock > '2023-09-23' UNION ALL SELECT id, last_restock, product_id FROM inventory WHERE quantity < 181
