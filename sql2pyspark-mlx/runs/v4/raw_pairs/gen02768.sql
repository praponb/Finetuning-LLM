SELECT is_remote, ROUND(AVG(salary), 2) avg_salary, COUNT(DISTINCT salary) unique_levels
FROM employees
GROUP BY 1
HAVING SUM(salary) > 640000
