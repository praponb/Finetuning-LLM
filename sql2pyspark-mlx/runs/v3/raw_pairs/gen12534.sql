SELECT email, name
FROM customers
WHERE segment = 'Consumer'
UNION ALL
SELECT email, name
FROM customers
WHERE segment = 'Corporate';
