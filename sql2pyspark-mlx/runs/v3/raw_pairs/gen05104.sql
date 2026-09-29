SELECT id, store_id, last_restock, COALESCE(quantity, 0) AS quantity_filled FROM inventory WHERE last_restock > '2024-10-01' AND quantity = 159 ORDER BY quantity_filled DESC, id ASC LIMIT 1;
