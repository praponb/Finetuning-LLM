SELECT e.id, e.hire_date, e.job_title, e.email FROM employees e WHERE e.job_title = 'Manager' AND e.dept_id IN (SELECT id FROM departments WHERE dept_name IS NOT NULL)
