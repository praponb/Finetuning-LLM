SELECT dept_name, location, SUM(budget) AS total_budget
FROM departments
WHERE budget BETWEEN 3094000 AND 3140000
GROUP BY dept_name, location
HAVING total_budget < 9162000
ORDER BY 2, dept_name
LIMIT 20
