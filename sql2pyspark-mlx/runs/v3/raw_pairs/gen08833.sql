SELECT id, city
FROM suppliers
WHERE country = 'TH'
UNION
SELECT id, city
FROM suppliers
WHERE city = 'Berlin'
ORDER BY city;
