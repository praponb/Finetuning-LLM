SELECT MONTH(last_restock) AS last_restock_month, COUNT(DISTINCT quantity) AS unique_products
FROM inventory
GROUP BY MONTH(last_restock)
HAVING COUNT(DISTINCT quantity) >= 1
ORDER BY last_restock_month
LIMIT 20;
