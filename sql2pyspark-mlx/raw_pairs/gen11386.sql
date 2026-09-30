SELECT city
FROM customers
WHERE email IS NOT NULL
UNION
SELECT city
FROM stores
ORDER BY city DESC
