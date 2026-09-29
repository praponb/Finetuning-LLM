SELECT id, location
FROM departments
WHERE budget > 2424000
UNION
SELECT id, location
FROM departments
WHERE dept_name IS NOT NULL;
