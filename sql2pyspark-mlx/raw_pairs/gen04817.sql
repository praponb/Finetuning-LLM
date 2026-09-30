SELECT product_id, MIN(quantity) AS min_quantity
FROM inventory
WHERE product_id IN (3, 6, 9, 10) AND quantity <> 194
GROUP BY product_id
HAVING AVG(quantity) >= 170;
