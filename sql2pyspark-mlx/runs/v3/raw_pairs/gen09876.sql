SELECT departments.dept_name, departments.location, employees.job_title, employees.salary FROM departments LEFT OUTER JOIN employees ON employees.dept_id = departments.id WHERE employees.dept_id = 6;
