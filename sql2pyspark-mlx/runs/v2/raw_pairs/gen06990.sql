SELECT
    employees.id,
    employees.full_name,
    employees.salary,
    departments.dept_name,
    departments.location
FROM employees
INNER JOIN departments ON employees.dept_id = departments.id
WHERE employees.salary < 65000 OR departments.dept_name != 'Engineering';
