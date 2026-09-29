SELECT employees.id, employees.job_title, employees.email, departments.location, departments.dept_name FROM employees LEFT JOIN departments ON employees.dept_id = departments.id;
