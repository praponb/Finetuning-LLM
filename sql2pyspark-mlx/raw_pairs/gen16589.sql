SELECT d.id, COUNT(e.id) AS employee_count
FROM departments d
INNER JOIN employees e ON d.id = e.dept_id
GROUP BY d.id
ORDER BY 2 DESC
