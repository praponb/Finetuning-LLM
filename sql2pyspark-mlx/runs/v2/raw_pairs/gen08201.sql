SELECT COUNT(quantity) AS quantity_count, MIN(quantity) AS min_quantity, AVG(quantity) AS avg_quantity
FROM inventory
WHERE product_id IN (2, 9, 10)
ORDER BY min_quantity DESC;
