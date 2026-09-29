SELECT city, 'customer' origin
FROM customers
UNION
SELECT city, 'store' origin
FROM stores
ORDER BY origin;
