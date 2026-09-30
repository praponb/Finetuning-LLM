SELECT id, last_restock FROM inventory WHERE last_restock = (SELECT MAX(last_restock) FROM inventory) ORDER BY last_restock
