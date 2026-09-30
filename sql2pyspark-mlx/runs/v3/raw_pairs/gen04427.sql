SELECT departments.id, departments.dept_name, departments.budget
FROM departments
WHERE departments.location <> 'Tokyo' AND EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id);
