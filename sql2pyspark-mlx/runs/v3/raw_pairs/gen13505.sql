SELECT dept_name, COUNT(budget) AS budget_count, AVG(budget) AS avg_budget FROM departments GROUP BY dept_name ORDER BY budget_count ASC
