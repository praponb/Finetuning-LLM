SELECT city
FROM customers
WHERE email LIKE '%@startup.io'
EXCEPT
SELECT city
FROM stores;
