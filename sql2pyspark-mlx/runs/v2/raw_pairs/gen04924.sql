SELECT d.dept_name, d.location, e.email, e.is_remote FROM departments d INNER JOIN employees e ON e.dept_id = d.id WHERE d.dept_name != 'Legal' AND e.dept_id = 22 ORDER BY e.email DESC;
