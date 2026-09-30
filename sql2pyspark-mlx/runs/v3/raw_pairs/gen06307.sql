SELECT name, city
FROM customers
WHERE age != 63
UNION ALL
SELECT name, city
FROM customers
WHERE country != 'SG'
ORDER BY name DESC
