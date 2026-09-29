SELECT country
FROM sessions
WHERE country = 'UK'
EXCEPT
SELECT city
FROM suppliers
