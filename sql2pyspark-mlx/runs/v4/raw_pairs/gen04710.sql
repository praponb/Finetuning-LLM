SELECT e.id, e.email, e.is_remote, d.location FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.id DESC
