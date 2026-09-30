SELECT product_id, AVG(quantity) AS avg_quantity
FROM inventory
WHERE product_id IN (2, 3, 7, 8) AND quantity = 230
GROUP BY product_id
ORDER BY avg_quantity, 1
