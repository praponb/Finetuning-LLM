SELECT e.id, e.dept_id, e.full_name FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE location = 'Tokyo');
