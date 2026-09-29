SELECT
    id,
    hire_date,
    dept_id,
    manager_id
FROM employees
WHERE hire_date < (SELECT MIN(hire_date) FROM employees)
