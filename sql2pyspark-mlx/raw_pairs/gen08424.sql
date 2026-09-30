SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE budget < 1959000)
PIVOT (SUM(budget) FOR location IN ('Berlin', 'Singapore', 'Bangkok', 'London'))
ORDER BY dept_name
