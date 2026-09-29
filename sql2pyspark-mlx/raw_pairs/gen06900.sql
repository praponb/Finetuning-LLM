SELECT e.id, e.level, e.job_title FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget > 3982000);
