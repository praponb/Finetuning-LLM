SELECT
    employees.id,
    employees.manager_id,
    departments.location,
    departments.dept_name,
    departments.budget
FROM employees
INNER JOIN departments ON departments.id = employees.dept_id;
