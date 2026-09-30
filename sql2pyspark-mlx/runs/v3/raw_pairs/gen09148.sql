SELECT
    employees.id,
    employees.manager_id,
    employees.salary,
    employees.level
FROM employees
WHERE employees.dept_id IN (SELECT id FROM departments WHERE dept_name IS NOT NULL)
ORDER BY employees.id, manager_id;
