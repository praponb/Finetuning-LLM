SELECT location, dept_name
FROM departments
WHERE location = 'London'
UNION ALL
SELECT location, dept_name
FROM departments
WHERE location = 'New York'
ORDER BY dept_name DESC;
