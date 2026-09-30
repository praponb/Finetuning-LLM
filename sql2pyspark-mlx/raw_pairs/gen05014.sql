SELECT id, last_restock, quantity
FROM inventory
WHERE last_restock = (SELECT MIN(last_restock) FROM inventory WHERE quantity < 181)
ORDER BY 1 DESC
