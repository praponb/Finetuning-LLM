SELECT e.id, e.job_title, e.manager_id FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE budget >= 2402000) ORDER BY id LIMIT 10;
