SELECT e.id, e.is_remote, e.job_title, e.full_name, d.dept_name FROM employees e JOIN departments d ON d.id = e.dept_id ORDER BY dept_name DESC, id LIMIT 3;
