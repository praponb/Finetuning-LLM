SELECT
    employees.id,
    employees.salary,
    employees.is_remote,
    departments.dept_name,
    departments.budget,
    departments.location
FROM employees
LEFT JOIN departments ON employees.dept_id = departments.id
