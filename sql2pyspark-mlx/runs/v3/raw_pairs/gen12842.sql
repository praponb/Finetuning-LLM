SELECT city, 'customer' AS source
FROM customers
WHERE LOWER(TRIM(name)) = 'david lee'
UNION
SELECT city, 'store' AS source
FROM stores;
