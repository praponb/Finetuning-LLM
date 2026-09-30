SELECT AVG(quantity) AS avg_quantity
FROM inventory
WHERE product_id IN (5, 10)
ORDER BY avg_quantity DESC;
