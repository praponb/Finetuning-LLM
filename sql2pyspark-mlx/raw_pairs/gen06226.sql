SELECT price, 'product' AS source
FROM products
UNION
SELECT amount AS price, 'order' AS source
FROM orders
ORDER BY source;
