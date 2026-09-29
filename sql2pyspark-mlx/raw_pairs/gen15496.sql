SELECT d.id, d.dept_name FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE level IN ('L1', 'L2'));
