SELECT product_id, AVG(quantity) AS avg_quantity, SUM(quantity) AS total_quantity
FROM inventory
WHERE store_id IN (3, 4, 5, 6)
GROUP BY product_id
HAVING total_quantity > 1230;
