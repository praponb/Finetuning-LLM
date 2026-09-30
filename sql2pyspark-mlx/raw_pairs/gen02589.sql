SELECT 'customer' AS origin, city
FROM customers
WHERE credit_limit BETWEEN 5700 AND 11200
UNION ALL
SELECT 'store' AS origin, city
FROM stores
WHERE city = 'Bangkok'
ORDER BY city;
