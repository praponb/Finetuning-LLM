SELECT d.dept_name, d.location, d.budget, e.job_title, e.salary, e.email FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id
