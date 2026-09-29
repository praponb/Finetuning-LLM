SELECT
    id,
    dept_id,
    manager_id,
    full_name
FROM employees
WHERE level = 'L1'
ORDER BY dept_id DESC;
