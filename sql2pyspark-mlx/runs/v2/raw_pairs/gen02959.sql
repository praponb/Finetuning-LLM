SELECT d.dept_name, d.location, e.email, e.is_remote FROM departments d LEFT JOIN employees e ON e.dept_id = d.id;
