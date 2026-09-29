SELECT * FROM (SELECT location, dept_name, budget FROM departments)
PIVOT (SUM(budget) FOR dept_name IN ('Legal', 'HR'))
ORDER BY location
