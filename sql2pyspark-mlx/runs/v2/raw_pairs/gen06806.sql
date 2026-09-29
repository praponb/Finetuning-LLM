SELECT COUNT(quantity) AS quantity_count, AVG(quantity) AS avg_quantity
FROM inventory
WHERE quantity = 153
ORDER BY quantity_count DESC;
