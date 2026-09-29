SELECT id, last_restock, product_id, quantity FROM inventory WHERE last_restock = (SELECT MAX(last_restock) FROM inventory);
