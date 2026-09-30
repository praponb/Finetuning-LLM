SELECT id, last_restock, store_id, quantity FROM inventory WHERE quantity >= 191 ORDER BY store_id, id LIMIT 10
