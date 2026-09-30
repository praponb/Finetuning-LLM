SELECT SUM(quantity) AS total_quantity, MIN(last_restock) AS min_last_restock
FROM inventory
ORDER BY 2 DESC, total_quantity DESC
