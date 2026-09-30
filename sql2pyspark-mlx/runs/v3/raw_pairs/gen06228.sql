SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE dept_name = 'HR')
PIVOT (MAX(budget) FOR dept_name IN ('Finance', 'Sales'))
ORDER BY location;
