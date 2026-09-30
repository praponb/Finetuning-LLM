SELECT e.id, e.salary, e.dept_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Engineering');
