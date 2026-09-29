SELECT e.id, e.hire_date, e.dept_id, e.level FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Legal');
