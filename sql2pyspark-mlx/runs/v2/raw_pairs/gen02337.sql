SELECT d.location, d.dept_name, e.manager_id, e.level, e.full_name FROM departments d INNER JOIN employees e ON e.dept_id = d.id ORDER BY manager_id DESC, full_name;
