SELECT COUNT(DISTINCT quantity) AS unique_stores, COUNT(quantity) AS quantity_count, AVG(quantity) AS avg_quantity FROM inventory ORDER BY avg_quantity DESC
