SELECT
    id,
    job_title,
    salary,
    full_name,
    FIRST_VALUE(full_name) OVER (PARTITION BY job_title ORDER BY salary DESC, id) first_full_name
FROM employees
WHERE level = 'L1'
ORDER BY id DESC
