SELECT e.id, e.dept_id, e.job_title FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE budget >= 2885000) ORDER BY e.id DESC LIMIT 5
