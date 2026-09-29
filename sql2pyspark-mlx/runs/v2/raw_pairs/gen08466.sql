SELECT product_id, SUM(quantity) AS total_quantity, COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
WHERE quantity < 217
GROUP BY product_id;
