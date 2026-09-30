SELECT e.id, e.is_remote FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE budget BETWEEN 1942000 AND 2733000) ORDER BY id DESC;
