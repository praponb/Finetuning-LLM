SELECT employees.id, employees.email, employees.full_name FROM employees WHERE employees.dept_id IN (SELECT id FROM departments WHERE dept_name IN ('Engineering', 'HR', 'Sales'));
