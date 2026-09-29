SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget <= 3008000)
PIVOT (MAX(budget) FOR dept_name IN ('Legal', 'Sales'))
ORDER BY location
