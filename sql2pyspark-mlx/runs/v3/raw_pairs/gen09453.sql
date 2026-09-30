SELECT
    employees.id,
    employees.job_title,
    employees.email,
    departments.location,
    departments.budget,
    departments.dept_name
FROM employees
JOIN departments ON employees.dept_id = departments.id;
