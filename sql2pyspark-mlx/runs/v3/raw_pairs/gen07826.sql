SELECT e.id, e.full_name, e.dept_id, e.is_remote, d.location FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY full_name, location
