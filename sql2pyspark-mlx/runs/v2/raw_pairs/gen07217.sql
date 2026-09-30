SELECT
    id,
    is_remote,
    level,
    job_title,
    SUBSTRING(full_name, 1, 2) AS full_name_prefix,
    DAYOFWEEK(hire_date) AS hire_date_dayofweek
FROM employees
WHERE email LIKE '%@outlook.com'
