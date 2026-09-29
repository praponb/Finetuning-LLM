SELECT * FROM (SELECT dept_name, location, budget FROM departments) PIVOT (MAX(budget) FOR location IN ('New York', 'Berlin', 'Singapore', 'Tokyo'));
