SELECT full_name
FROM employees
EXCEPT
SELECT name
FROM customers
ORDER BY full_name;
