SELECT city, 'customer' AS source
FROM customers
UNION
SELECT city, 'store' AS source
FROM stores
ORDER BY source;
