SELECT departments.id, departments.budget, departments.location, departments.dept_name FROM departments WHERE NOT EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id);
