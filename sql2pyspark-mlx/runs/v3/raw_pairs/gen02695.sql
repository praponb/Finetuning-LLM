SELECT id, last_restock FROM inventory WHERE last_restock = (SELECT MAX(last_restock) FROM inventory WHERE MONTH(last_restock) IN (5, 8, 12))
