SELECT * FROM (SELECT dept_name, location, budget FROM departments WHERE dept_name NOT IN ('Sales', 'Legal')) PIVOT (SUM(budget) FOR location IN ('London', 'Bangkok', 'Berlin', 'Singapore'));
