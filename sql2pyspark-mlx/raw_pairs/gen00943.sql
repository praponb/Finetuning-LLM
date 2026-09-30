SELECT country
FROM customers
EXCEPT
SELECT email
FROM employees
ORDER BY country DESC;
