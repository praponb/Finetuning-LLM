SELECT
    id,
    salary,
    level,
    full_name
FROM employees
WHERE LENGTH(full_name) > 14 AND salary <= 61000
