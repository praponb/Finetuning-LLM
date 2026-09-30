SELECT d.dept_name, e.email, e.job_title, e.dept_id, DATE_SUB(e.hire_date, 60) AS hire_date_minus_60d FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE e.job_title = 'Engineer'
