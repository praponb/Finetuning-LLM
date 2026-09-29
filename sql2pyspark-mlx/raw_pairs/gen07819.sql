SELECT
    id,
    hire_date,
    email,
    CONCAT(job_title, '/', level) AS job_title_level
FROM employees
WHERE hire_date < '2024-12-01'
ORDER BY job_title_level, email;
