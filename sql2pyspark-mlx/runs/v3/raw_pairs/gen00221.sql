SELECT departments.location, employees.dept_id, employees.email FROM departments LEFT JOIN employees ON departments.id = employees.dept_id
