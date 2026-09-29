SELECT departments.id, departments.location, departments.dept_name
FROM departments
WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id AND MONTH(employees.hire_date) IN (3, 7, 9))
ORDER BY departments.dept_name, id
LIMIT 50
