SELECT id, quantity, store_id, last_restock FROM inventory WHERE last_restock >= '2024-02-18' AND (store_id IN (2, 4, 5, 6) OR store_id IN (3, 4));
