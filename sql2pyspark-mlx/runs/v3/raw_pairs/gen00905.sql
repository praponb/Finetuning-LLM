SELECT d.id, d.budget FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE LOWER(TRIM(full_name)) = 'david lee')
