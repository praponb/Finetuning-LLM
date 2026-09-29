SELECT
    employees.id,
    employees.is_remote,
    employees.job_title,
    employees.full_name
FROM employees
WHERE employees.dept_id NOT IN (SELECT id FROM departments WHERE location != 'London');
