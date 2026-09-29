SELECT dept_name, location
FROM departments
WHERE budget > 2045000
UNION
SELECT dept_name, location
FROM departments
WHERE dept_name IN ('HR', 'Marketing', 'Engineering')
