SELECT e.id, e.hire_date, e.is_remote FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location = 'Singapore');
