SELECT product_id, COUNT(DISTINCT quantity) AS unique_products FROM inventory WHERE product_id = 4 GROUP BY product_id HAVING COUNT(DISTINCT quantity) >= 5 ORDER BY 2 DESC;
