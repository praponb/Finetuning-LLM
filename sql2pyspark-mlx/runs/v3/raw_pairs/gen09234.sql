SELECT * FROM (SELECT location, dept_name, budget FROM departments)
PIVOT (COUNT(budget) FOR dept_name IN ('Marketing', 'Engineering'))
ORDER BY location;
