SELECT e.id, e.full_name FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location = 'London');
