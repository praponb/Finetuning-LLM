SELECT e.id, e.job_title, d.budget FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY budget
