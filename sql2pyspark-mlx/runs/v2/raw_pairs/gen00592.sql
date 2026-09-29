SELECT id, store_id FROM inventory WHERE quantity <= 155 INTERSECT SELECT id, store_id FROM inventory WHERE last_restock > '2024-04-04';
