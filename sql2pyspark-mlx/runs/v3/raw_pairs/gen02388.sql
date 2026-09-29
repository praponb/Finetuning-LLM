SELECT d.location, e.full_name, e.email, e.level FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id WHERE e.job_title IN ('Analyst', 'Intern');
