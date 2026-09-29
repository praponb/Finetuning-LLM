SELECT d.id, d.dept_name, d.budget FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE LOWER(email) LIKE '%avi%')
