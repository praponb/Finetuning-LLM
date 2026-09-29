SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary, MAX(salary) AS max_salary
FROM employees
WHERE hire_date <= '2025-05-01' AND manager_id IN (18, 19, 24)
GROUP BY 1
ORDER BY dept_id DESC
