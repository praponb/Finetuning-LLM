SELECT departments.id, departments.location FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id AND employees.level IN ('L2', 'L4', 'L1'));
