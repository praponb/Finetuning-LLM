SELECT e.is_remote, d.location FROM employees e LEFT JOIN departments d ON d.id = e.dept_id;
