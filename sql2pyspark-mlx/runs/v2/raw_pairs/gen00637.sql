SELECT dept_name
FROM departments
WHERE dept_name = 'Engineering'
UNION
SELECT dept_name
FROM departments
WHERE location = 'Chiang Mai';
