SELECT amount, 'order' AS source
FROM orders
UNION ALL
SELECT price, 'product' AS source
FROM products;
