SELECT e.id, e.level, e.job_title, e.is_remote, d.dept_name, d.budget, d.location FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY id DESC, budget LIMIT 5;
