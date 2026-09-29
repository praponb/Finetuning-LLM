SELECT e.full_name, e.job_title, d.location, d.budget, d.dept_name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id;
