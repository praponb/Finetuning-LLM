SELECT id, last_restock, quantity, LAG(quantity, 1) OVER (ORDER BY last_restock, id) prev_quantity FROM inventory;
