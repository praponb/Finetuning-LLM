SELECT SUM(budget) total_budget
FROM departments
WHERE dept_name = 'Marketing'
ORDER BY total_budget
