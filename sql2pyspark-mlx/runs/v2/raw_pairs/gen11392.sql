SELECT d.dept_name, e.is_remote
FROM departments d
JOIN employees e ON d.id = e.dept_id;
