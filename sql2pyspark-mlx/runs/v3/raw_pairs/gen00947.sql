SELECT id, location, dept_name
FROM departments
WHERE location = 'Berlin'
UNION ALL
SELECT id, location, dept_name
FROM departments
WHERE budget > 3932000
