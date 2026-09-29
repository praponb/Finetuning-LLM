SELECT d.location, e.job_title, e.email FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id ORDER BY email
