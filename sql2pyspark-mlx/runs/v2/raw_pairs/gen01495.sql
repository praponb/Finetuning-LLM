SELECT id, age, country
FROM customers
WHERE country = 'DE'
UNION ALL
SELECT id, age, country
FROM customers
WHERE city = 'Berlin'
ORDER BY country;
