SELECT id, location
FROM departments
INTERSECT
SELECT id, location
FROM departments
WHERE dept_name = 'Legal'
