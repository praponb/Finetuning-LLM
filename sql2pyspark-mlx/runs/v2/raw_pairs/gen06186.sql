SELECT id, last_restock, store_id FROM inventory WHERE quantity <> 61 UNION ALL SELECT id, last_restock, store_id FROM inventory WHERE last_restock >= '2025-01-20' ORDER BY last_restock;
