SELECT product_id, AVG(quantity) avg_quantity
FROM inventory
WHERE quantity <= 213
GROUP BY 1
HAVING AVG(quantity) > 120;
