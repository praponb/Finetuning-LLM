SELECT d.dept_name, COUNT(*) employee_count FROM employees e INNER JOIN departments d ON e.dept_id = d.id GROUP BY d.dept_name ORDER BY dept_name
