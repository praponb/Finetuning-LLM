SELECT country
FROM customers
EXCEPT
SELECT country
FROM sessions
WHERE browser IN ('Edge', 'Safari', 'Firefox')
ORDER BY country DESC;
