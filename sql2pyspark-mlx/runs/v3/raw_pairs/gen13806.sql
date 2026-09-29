SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE location = 'Singapore')
PIVOT (AVG(budget) FOR location IN ('Singapore', 'New York', 'Tokyo', 'Chiang Mai'));
