SELECT d.id, d.dept_name FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE UPPER(TRIM(full_name)) = 'CAROL DIAZ') ORDER BY d.id
