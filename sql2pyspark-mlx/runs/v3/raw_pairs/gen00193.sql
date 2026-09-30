SELECT d.budget, d.dept_name, e.job_title, LENGTH(d.location) location_length FROM departments d LEFT JOIN employees e ON d.id = e.dept_id ORDER BY dept_name
