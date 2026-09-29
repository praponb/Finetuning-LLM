SELECT YEAR(last_restock) AS last_restock_year, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
GROUP BY 1
HAVING MAX(quantity) < 220;
