SELECT email, 'customer' AS source
FROM customers
UNION ALL
SELECT level, 'employee' AS source
FROM employees
WHERE hire_date < '2023-04-12'
ORDER BY email;
