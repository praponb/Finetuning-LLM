SELECT id, last_restock, quantity, product_id FROM inventory WHERE last_restock = (SELECT MIN(last_restock) FROM inventory) ORDER BY quantity ASC, product_id ASC;
