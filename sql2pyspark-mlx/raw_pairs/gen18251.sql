SELECT
    employees.dept_id,
    employees.hire_date,
    employees.manager_id,
    departments.budget
FROM employees
INNER JOIN departments ON employees.dept_id = departments.id
WHERE departments.budget > 3836000;
