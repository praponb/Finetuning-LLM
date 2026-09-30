SELECT id, last_restock
FROM inventory
WHERE last_restock < (SELECT MIN(last_restock) FROM inventory)
