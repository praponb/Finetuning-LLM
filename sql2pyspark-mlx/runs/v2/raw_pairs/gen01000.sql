SELECT e.full_name, d.location, d.dept_name, d.budget, CAST(e.is_remote AS INT) is_remote_flag FROM employees e LEFT JOIN departments d ON d.id = e.dept_id;
