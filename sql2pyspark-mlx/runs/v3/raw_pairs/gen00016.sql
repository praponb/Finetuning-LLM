SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, MIN(quantity) AS min_quantity
FROM inventory
WHERE last_restock < '2025-11-01'
GROUP BY last_restock_month;
