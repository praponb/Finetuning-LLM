SELECT departments.dept_name, departments.location, employees.dept_id
FROM departments
LEFT JOIN employees ON departments.id = employees.dept_id;
