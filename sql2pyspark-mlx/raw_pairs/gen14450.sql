SELECT e.id, e.dept_id, e.job_title FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE budget IS NOT NULL) ORDER BY e.id DESC, job_title;
