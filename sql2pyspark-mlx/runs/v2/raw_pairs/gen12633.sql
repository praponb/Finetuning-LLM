SELECT location, dept_name
FROM departments
WHERE budget > 1867000
INTERSECT
SELECT location, dept_name
FROM departments
WHERE dept_name = 'Engineering'
