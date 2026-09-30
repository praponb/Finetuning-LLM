SELECT dept_name, COUNT(budget) AS budget_count FROM departments GROUP BY dept_name HAVING COUNT(budget) < 3
