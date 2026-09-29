SELECT e.id, e.is_remote, d.location, d.dept_name, d.budget FROM employees e JOIN departments d ON e.dept_id = d.id WHERE LENGTH(e.full_name) > 13 AND LENGTH(e.full_name) >= 12
