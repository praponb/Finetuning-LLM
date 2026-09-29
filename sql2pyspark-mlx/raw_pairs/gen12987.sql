SELECT location
FROM departments
WHERE dept_name IN ('HR', 'Sales')
UNION ALL
SELECT email
FROM customers
WHERE age >= 56
ORDER BY location
