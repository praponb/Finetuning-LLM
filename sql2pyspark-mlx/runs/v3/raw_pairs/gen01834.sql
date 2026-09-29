SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, ROUND(AVG(quantity), 2) AS avg_quantity
FROM inventory
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
ORDER BY 1
LIMIT 5;
