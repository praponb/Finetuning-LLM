SELECT e.hire_date, e.job_title, d.location, d.dept_name, d.budget FROM employees e FULL OUTER JOIN departments d ON e.dept_id = d.id WHERE d.budget <= 4004000
