SELECT d.location, d.dept_name, e.salary, e.dept_id, e.is_remote, LENGTH(e.job_title) AS job_title_length FROM departments d INNER JOIN employees e ON e.dept_id = d.id
