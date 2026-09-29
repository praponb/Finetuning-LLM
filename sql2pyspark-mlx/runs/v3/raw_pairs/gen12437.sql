SELECT e.id, e.job_title, e.hire_date, d.dept_name FROM employees e INNER JOIN departments d ON e.dept_id = d.id WHERE e.level IN ('L4', 'L1');
