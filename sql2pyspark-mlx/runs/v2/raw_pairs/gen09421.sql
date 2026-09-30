SELECT d.dept_name, SUM(e.salary) AS total_salary
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE e.salary BETWEEN 109000 AND 146000
GROUP BY d.dept_name
HAVING total_salary >= 289000
ORDER BY total_salary DESC, dept_name
LIMIT 3;
