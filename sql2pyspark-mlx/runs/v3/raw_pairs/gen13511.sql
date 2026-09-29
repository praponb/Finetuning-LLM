SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE budget <= 3650000) PIVOT (COUNT(budget) FOR location IN ('London', 'Singapore'))
