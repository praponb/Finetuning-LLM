SELECT d.budget, d.dept_name, d.location, e.job_title, e.email FROM departments d LEFT OUTER JOIN employees e ON d.id = e.dept_id WHERE NOT e.is_remote ORDER BY email
