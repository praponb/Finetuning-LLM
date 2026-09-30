SELECT id, store_id, product_id, last_restock, quantity, SUM(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) AS running_quantity FROM inventory
