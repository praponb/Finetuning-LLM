SELECT e.id, e.manager_id, e.dept_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget BETWEEN 2643000 AND 3356000) ORDER BY manager_id DESC, dept_id, id LIMIT 20
