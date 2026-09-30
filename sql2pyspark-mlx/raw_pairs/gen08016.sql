SELECT dept_name, SUM(budget) total_budget, MAX(budget) max_budget, ROUND(AVG(budget), 2) avg_budget FROM departments GROUP BY dept_name
