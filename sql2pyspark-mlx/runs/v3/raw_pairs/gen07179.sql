SELECT
    id,
    level,
    full_name,
    hire_date
FROM employees
WHERE manager_id IN (1, 10, 16);
