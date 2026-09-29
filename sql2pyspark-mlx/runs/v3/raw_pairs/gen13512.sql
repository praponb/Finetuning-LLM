SELECT amount
FROM orders
UNION
SELECT cost
FROM products
ORDER BY amount DESC
