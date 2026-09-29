SELECT id, dept_name, location
FROM departments
WHERE location IN ('Singapore', 'Tokyo', 'Bangkok')
UNION ALL
SELECT id, dept_name, location
FROM departments
WHERE budget BETWEEN 1326000 AND 1845000
ORDER BY dept_name DESC;
