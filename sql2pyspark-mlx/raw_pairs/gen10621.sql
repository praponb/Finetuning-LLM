SELECT id, location, dept_name, budget, SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget FROM departments;
