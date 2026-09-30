SELECT MIN(quantity) AS min_quantity, AVG(quantity) AS avg_quantity, COUNT(quantity) AS quantity_count FROM inventory ORDER BY quantity_count DESC;
