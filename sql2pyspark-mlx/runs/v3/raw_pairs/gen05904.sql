SELECT e.id, e.dept_id, e.manager_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Engineering') ORDER BY id DESC LIMIT 5;
