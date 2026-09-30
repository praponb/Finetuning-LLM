SELECT name, country
FROM customers
WHERE email IS NOT NULL
UNION
SELECT name, country
FROM customers
WHERE email IS NOT NULL;
