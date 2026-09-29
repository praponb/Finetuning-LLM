SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, MIN(quantity) AS min_quantity
FROM inventory
GROUP BY product_id
HAVING COUNT(*) < 2
