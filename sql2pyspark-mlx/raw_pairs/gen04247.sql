SELECT product_id, SUM(quantity) AS total_quantity, MIN(quantity) AS min_quantity
FROM inventory
WHERE (quantity BETWEEN 73 AND 145 AND product_id IN (4, 7, 10)) OR quantity < 67
GROUP BY product_id;
