SELECT location
FROM departments
WHERE dept_name = 'Sales'
EXCEPT
SELECT location
FROM departments
WHERE budget <= 2995000;
