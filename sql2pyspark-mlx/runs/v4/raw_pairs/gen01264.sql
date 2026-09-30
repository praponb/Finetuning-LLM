SELECT ROUND(AVG(quantity), 2) AS avg_quantity, COUNT(quantity) AS quantity_count, MIN(quantity) AS min_quantity FROM inventory ORDER BY quantity_count
