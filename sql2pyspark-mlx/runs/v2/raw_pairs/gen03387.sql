SELECT departments.id, departments.dept_name
FROM departments
WHERE departments.location = 'London' AND NOT EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = departments.id AND employees.job_title IN ('Analyst', 'Manager'));
