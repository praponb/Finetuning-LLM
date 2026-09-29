SELECT * FROM (SELECT dept_name, location, budget FROM departments)
PIVOT (AVG(budget) FOR location IN ('New York', 'Chiang Mai', 'Singapore'))
ORDER BY dept_name;
