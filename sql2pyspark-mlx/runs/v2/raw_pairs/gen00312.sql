SELECT SUM(quantity) AS total_quantity, MIN(last_restock) AS min_last_restock
FROM inventory
ORDER BY min_last_restock;
