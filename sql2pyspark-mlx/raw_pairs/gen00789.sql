SELECT e.id, e.hire_date, e.job_title, d.budget FROM employees e JOIN departments d ON d.id = e.dept_id WHERE e.email IS NOT NULL AND d.dept_name != 'Finance'
