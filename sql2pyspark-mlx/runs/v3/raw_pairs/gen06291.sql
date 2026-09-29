SELECT amount AS value
FROM orders
INTERSECT
SELECT revenue AS value
FROM store_sales
WHERE units BETWEEN 21 AND 24
