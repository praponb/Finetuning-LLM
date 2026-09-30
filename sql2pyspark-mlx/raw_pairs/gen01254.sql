SELECT id, dept_name
FROM departments
WHERE location <> 'London'
UNION ALL
SELECT id, dept_name
FROM departments
WHERE location IN ('New York', 'Bangkok')
ORDER BY dept_name DESC;
