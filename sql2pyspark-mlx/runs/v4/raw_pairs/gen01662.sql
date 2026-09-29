SELECT country
FROM suppliers
WHERE rating <= 2.1
UNION
SELECT country
FROM sessions
