SELECT
    id,
    email,
    job_title,
    hire_date,
    SUBSTRING(full_name, 1, 4) AS full_name_prefix
FROM employees
WHERE NOT is_remote
ORDER BY full_name_prefix;
