SELECT
    salary,
    job_title,
    hire_date,
    UPPER(level) AS level_upper
FROM employees;
