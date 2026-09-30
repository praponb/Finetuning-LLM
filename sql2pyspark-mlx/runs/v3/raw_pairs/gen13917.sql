SELECT departments.id, departments.location, departments.budget
FROM departments
WHERE departments.location = 'Bangkok' AND NOT EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id)
ORDER BY location, departments.budget
