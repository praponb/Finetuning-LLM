SELECT e.id, e.level, e.job_title, d.budget, d.location, d.dept_name FROM employees e INNER JOIN departments d ON d.id = e.dept_id WHERE e.is_remote;
