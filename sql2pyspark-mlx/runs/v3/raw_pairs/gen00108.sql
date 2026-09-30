SELECT e.id, e.email, e.hire_date, e.manager_id FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name = 'Finance') ORDER BY e.hire_date ASC
