SELECT dept_name, location
FROM departments
WHERE dept_name = 'HR'
EXCEPT
SELECT dept_name, location
FROM departments
WHERE budget >= 3204000
ORDER BY location DESC;
