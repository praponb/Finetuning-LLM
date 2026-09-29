SELECT d.budget, d.dept_name, d.location, e.job_title, e.email FROM departments d INNER JOIN employees e ON d.id = e.dept_id ORDER BY location, e.job_title DESC;
