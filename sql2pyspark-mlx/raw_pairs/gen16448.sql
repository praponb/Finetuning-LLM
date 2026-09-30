SELECT segment, name
FROM customers
WHERE city = 'Chiang Mai'
EXCEPT
SELECT segment, name
FROM customers
WHERE email LIKE '%@gmail.com'
ORDER BY segment DESC;
