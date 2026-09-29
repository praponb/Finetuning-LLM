SELECT e.id, e.dept_id, d.dept_name, d.location FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY id DESC;
