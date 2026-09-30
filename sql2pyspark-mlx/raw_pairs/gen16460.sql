SELECT id, location, dept_name
FROM departments
WHERE dept_name = 'Finance'
INTERSECT
SELECT id, location, dept_name
FROM departments
WHERE budget >= 2400000;
