SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget > 1589000)
PIVOT (MAX(budget) FOR dept_name IN ('Marketing', 'Finance'));
