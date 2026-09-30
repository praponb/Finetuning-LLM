SELECT MAX(last_restock) AS last_last_restock
FROM inventory
WHERE last_restock < '2025-10-19'
ORDER BY last_last_restock
