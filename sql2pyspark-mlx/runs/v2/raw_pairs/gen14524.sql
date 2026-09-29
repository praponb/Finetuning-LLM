SELECT location, dept_name
FROM departments
WHERE location = 'New York'
UNION ALL
SELECT location, dept_name
FROM departments
WHERE budget BETWEEN 2299000 AND 2888000;
