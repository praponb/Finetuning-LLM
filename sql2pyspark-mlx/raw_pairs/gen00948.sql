SELECT 'customer' AS origin, name
FROM customers
WHERE city IS NOT NULL
UNION
SELECT 'employee' AS origin, full_name
FROM employees
WHERE salary <= 100000;
