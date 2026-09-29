SELECT price
FROM products
UNION
SELECT cost AS price
FROM shipments
ORDER BY price
