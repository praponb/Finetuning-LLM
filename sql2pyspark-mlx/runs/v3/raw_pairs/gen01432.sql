SELECT
    employees.id,
    employees.dept_id,
    departments.location,
    departments.dept_name,
    departments.budget
FROM employees
LEFT JOIN departments ON employees.dept_id = departments.id;
