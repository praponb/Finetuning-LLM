SELECT departments.budget, departments.location, departments.dept_name, employees.email FROM departments INNER JOIN employees ON departments.id = employees.dept_id ORDER BY location;
