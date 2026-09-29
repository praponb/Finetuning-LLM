SELECT location, dept_name
FROM departments
EXCEPT
SELECT location, dept_name
FROM departments
WHERE dept_name IN ('Sales', 'Legal')
