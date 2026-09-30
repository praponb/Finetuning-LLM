SELECT id, store_id, last_restock, quantity, SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id) running_quantity FROM inventory
