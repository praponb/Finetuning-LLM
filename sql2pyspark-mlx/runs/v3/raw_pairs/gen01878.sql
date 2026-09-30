SELECT e.is_remote, e.level, e.job_title, d.dept_name, d.budget, d.location FROM employees e INNER JOIN departments d ON d.id = e.dept_id WHERE d.location = 'Berlin' AND e.hire_date < '2024-03-06';
