SELECT e.dept_id, e.job_title, d.dept_name, d.budget FROM employees e INNER JOIN departments d ON e.dept_id = d.id;
