SELECT id, location, dept_name, budget, ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 3) AS budget_share FROM departments WHERE budget >= 1483000;
