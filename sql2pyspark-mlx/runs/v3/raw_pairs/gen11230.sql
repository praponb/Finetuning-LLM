SELECT dept_name, MIN(budget) AS min_budget FROM departments WHERE NOT (budget < 2689000 AND location IN ('New York', 'Chiang Mai', 'London')) GROUP BY dept_name;
