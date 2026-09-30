SELECT d.location, d.dept_name, d.budget, e.is_remote, e.job_title FROM departments d INNER JOIN employees e ON d.id = e.dept_id ORDER BY e.job_title ASC
