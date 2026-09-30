SELECT price
FROM products
INTERSECT
SELECT amount AS price
FROM payments
