SELECT location, dept_name
FROM departments
WHERE dept_name IN ('Marketing', 'Finance', 'HR')
ORDER BY location
