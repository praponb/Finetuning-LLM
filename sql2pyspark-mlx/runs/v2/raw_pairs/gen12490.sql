SELECT dept_id, is_remote, ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY dept_id, is_remote
ORDER BY avg_salary DESC
