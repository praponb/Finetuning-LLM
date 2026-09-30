SELECT d.id, d.location, d.dept_name, d.budget FROM departments d WHERE d.id IN (SELECT dept_id FROM employees) ORDER BY id;
