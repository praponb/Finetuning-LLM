SELECT id, quantity FROM inventory WHERE store_id IN (2, 5) EXCEPT SELECT id, quantity FROM inventory WHERE last_restock >= '2025-04-01';
