SELECT id, quantity, last_restock, ROUND(quantity * 1.07, 2) AS adjusted_quantity FROM inventory;
