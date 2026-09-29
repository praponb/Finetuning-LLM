SELECT e.id, e.is_remote, d.location FROM employees e INNER JOIN departments d ON e.dept_id = d.id;
