SELECT city
FROM customers
WHERE credit_limit > 9600
UNION
SELECT city
FROM stores
WHERE store_name LIKE '%Tow%'
ORDER BY city
