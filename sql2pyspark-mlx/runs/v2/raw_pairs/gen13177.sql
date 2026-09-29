SELECT * FROM (SELECT location, dept_name, budget FROM departments WHERE budget >= 1499000) PIVOT (SUM(budget) FOR dept_name IN ('Engineering', 'Sales'));
