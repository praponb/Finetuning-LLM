SELECT
    hire_date,
    salary,
    full_name,
    SUBSTRING(level, 1, 2) AS level_prefix
FROM employees
WHERE hire_date >= '2025-07-26' AND job_title <> 'Director'
ORDER BY level_prefix, hire_date
