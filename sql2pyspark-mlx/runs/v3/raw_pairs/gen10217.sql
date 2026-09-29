SELECT ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(quantity) AS quantity_count, COUNT(*) AS total_count
FROM inventory
WHERE product_id IN (1, 6)
ORDER BY avg_quantity, quantity_count DESC
