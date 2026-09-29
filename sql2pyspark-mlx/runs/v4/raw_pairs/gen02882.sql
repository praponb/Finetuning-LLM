SELECT product_id, COUNT(DISTINCT quantity) AS unique_stores FROM inventory GROUP BY product_id HAVING COUNT(DISTINCT quantity) > 2;
