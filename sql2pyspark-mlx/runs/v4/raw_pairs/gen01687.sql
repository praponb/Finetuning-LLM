SELECT id, dept_name, budget, ROUND(AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) budget_moving_avg_3 FROM departments
