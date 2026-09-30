SELECT 'department' AS origin, location
FROM departments
UNION
SELECT 'customer' AS origin, city
FROM customers
ORDER BY origin
