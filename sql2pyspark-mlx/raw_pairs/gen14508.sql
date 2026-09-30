SELECT city
FROM customers
UNION
SELECT city
FROM stores
WHERE store_name IS NOT NULL
