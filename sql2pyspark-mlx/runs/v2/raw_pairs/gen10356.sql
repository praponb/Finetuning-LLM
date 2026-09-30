SELECT dept_name, ROUND(AVG(budget), 2) avg_budget
FROM departments
GROUP BY dept_name
HAVING ROUND(AVG(budget), 2) >= 62
