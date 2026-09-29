SELECT location, ROUND(AVG(budget), 2) AS avg_budget FROM departments WHERE dept_name = 'HR' GROUP BY location HAVING ROUND(AVG(budget), 2) < 37;
