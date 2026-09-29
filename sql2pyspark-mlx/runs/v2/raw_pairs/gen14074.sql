SELECT city, 'customer' source
FROM customers
WHERE age BETWEEN 49 AND 52
UNION ALL
SELECT city, 'store' source
FROM stores
ORDER BY source;
