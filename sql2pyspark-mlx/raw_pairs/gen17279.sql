SELECT e.id, e.email, d.location, d.budget, d.dept_name FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY d.budget
