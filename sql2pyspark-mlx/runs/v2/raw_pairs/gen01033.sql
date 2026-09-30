SELECT id, last_restock, quantity, SUM(quantity) OVER (ORDER BY last_restock, id) AS running_quantity FROM inventory
