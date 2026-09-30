SELECT departments.id, departments.dept_name, departments.location
FROM departments
WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id);
