SELECT *
FROM inventory
WHERE last_restock >= '2024-12-08'
ORDER BY quantity DESC
