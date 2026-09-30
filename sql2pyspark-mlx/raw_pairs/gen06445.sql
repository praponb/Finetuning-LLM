SELECT d.dept_name, SUM(e.salary) AS total_salary, AVG(e.salary) AS avg_salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name
ORDER BY 3 DESC, dept_name
