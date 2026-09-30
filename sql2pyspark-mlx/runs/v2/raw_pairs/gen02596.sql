SELECT departments.id, departments.location, COUNT(employees.id) AS employee_count
FROM departments
JOIN employees ON employees.dept_id = departments.id
GROUP BY departments.id, departments.location
HAVING COUNT(employees.id) >= 1
