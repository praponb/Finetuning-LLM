SELECT country
FROM customers
EXCEPT
SELECT country
FROM sessions
