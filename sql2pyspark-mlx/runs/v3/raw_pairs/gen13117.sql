SELECT
    id,
    job_title,
    hire_date,
    UPPER(job_title) AS job_title_upper
FROM employees
WHERE hire_date <= '2025-05-27' AND full_name IS NULL;
