SELECT departments.budget, employees.hire_date, employees.full_name FROM departments LEFT JOIN employees ON departments.id = employees.dept_id;
