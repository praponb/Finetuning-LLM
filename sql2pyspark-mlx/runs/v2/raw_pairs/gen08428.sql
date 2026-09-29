SELECT e.id, e.salary, e.email FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name = 'Sales') ORDER BY e.email;
