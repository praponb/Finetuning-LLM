SELECT departments.id, departments.location, COUNT(employees.id) AS employee_count
FROM departments
LEFT JOIN employees ON employees.dept_id = departments.id
WHERE employees.salary <= 112000
GROUP BY departments.id, departments.location
ORDER BY id DESC;
