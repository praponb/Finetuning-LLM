SELECT e.id, e.is_remote, e.full_name, e.dept_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location = 'London')
