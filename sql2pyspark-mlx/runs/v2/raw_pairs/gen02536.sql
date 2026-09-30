SELECT MAX(last_restock) AS latest_last_restock, SUM(quantity) AS total_quantity, AVG(quantity) AS avg_quantity
FROM inventory
ORDER BY avg_quantity DESC, latest_last_restock;
