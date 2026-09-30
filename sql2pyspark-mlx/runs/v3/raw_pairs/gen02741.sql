SELECT CASE WHEN quantity >= 237 THEN 'high' WHEN quantity >= 65 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(last_restock) AS min_last_restock, MIN(quantity) AS min_quantity
FROM inventory
WHERE last_restock >= '2024-09-01' OR last_restock BETWEEN '2023-11-01' AND '2023-12-01'
GROUP BY 1
HAVING min_quantity >= 130;
