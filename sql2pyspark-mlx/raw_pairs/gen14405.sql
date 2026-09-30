SELECT
    id,
    salary,
    hire_date,
    SUBSTRING(full_name, 1, 2) AS full_name_prefix
FROM employees
WHERE dept_id = 30;
