SELECT dept_name, SUM(budget) AS total_budget, MIN(budget) AS min_budget
FROM departments
GROUP BY dept_name
ORDER BY total_budget
