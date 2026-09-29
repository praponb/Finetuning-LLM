SELECT e.id, e.job_title, e.hire_date, e.dept_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location = 'Bangkok');
