SELECT location, AVG(budget) AS avg_budget, ROUND(STDDEV(budget), 2) AS stddev_budget FROM departments WHERE dept_name IN ('Finance', 'Marketing') GROUP BY location
