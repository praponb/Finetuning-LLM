SELECT location, 'department' AS source
FROM departments
WHERE dept_name = 'Sales'
UNION ALL
SELECT city, 'customer' AS source
FROM customers
WHERE city = 'Bangkok'
