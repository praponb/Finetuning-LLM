SELECT d.budget, d.dept_name, d.location, e.manager_id, e.job_title, e.dept_id FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE d.dept_name IN ('Marketing', 'Finance');
