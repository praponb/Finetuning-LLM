SELECT price AS value
FROM products
UNION
SELECT cost AS value
FROM shipments
ORDER BY value;
