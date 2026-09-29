SELECT dept_name, COUNT(budget) AS budget_count
FROM departments
GROUP BY dept_name
ORDER BY dept_name
