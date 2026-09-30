SELECT dept_name, AVG(budget) AS avg_budget
FROM departments
WHERE dept_name = 'Engineering'
GROUP BY dept_name
ORDER BY 2
