SELECT d.id, d.dept_name, COUNT(e.id) AS employee_count
FROM departments d
INNER JOIN employees e ON e.dept_id = d.id
GROUP BY d.id, d.dept_name
ORDER BY dept_name, employee_count DESC;
