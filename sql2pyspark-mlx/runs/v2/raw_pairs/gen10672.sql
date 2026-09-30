SELECT e.id, e.job_title, e.dept_id, e.level FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location IS NOT NULL) ORDER BY job_title, e.id DESC;
