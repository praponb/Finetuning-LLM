SELECT id, location, dept_name
FROM departments
WHERE dept_name = 'Finance'
UNION ALL
SELECT id, location, dept_name
FROM departments
WHERE budget BETWEEN 1300000 AND 2546000
