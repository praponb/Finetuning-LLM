SELECT id, quantity, last_restock
FROM inventory
WHERE quantity < (SELECT MIN(quantity) FROM inventory WHERE quantity = 74)
ORDER BY last_restock;
