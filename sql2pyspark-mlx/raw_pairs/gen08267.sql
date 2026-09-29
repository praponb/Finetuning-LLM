SELECT d.id, d.budget, d.location, d.dept_name FROM departments d WHERE d.id IN (SELECT dept_id FROM employees);
