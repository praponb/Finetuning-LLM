SELECT location, dept_name
FROM departments
WHERE dept_name = 'Marketing'
EXCEPT
SELECT location, dept_name
FROM departments
WHERE dept_name = 'Engineering';
