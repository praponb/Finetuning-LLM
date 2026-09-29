SELECT id, dept_name, location
FROM departments
WHERE budget >= 3015000
UNION
SELECT id, dept_name, location
FROM departments
WHERE location <> 'London';
