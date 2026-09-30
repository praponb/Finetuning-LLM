SELECT d.id, d.location, d.dept_name FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE manager_id IN (1, 9));
