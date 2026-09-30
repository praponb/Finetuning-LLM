SELECT store_id, AVG(quantity) AS avg_quantity, COUNT(DISTINCT quantity) AS unique_products, MAX(quantity) AS max_quantity FROM inventory GROUP BY store_id HAVING COUNT(*) < 3
