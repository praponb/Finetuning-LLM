SELECT
    id,
    job_title,
    email,
    level,
    CONCAT(email, ' ', level) AS email_level
FROM employees
WHERE full_name LIKE 'K%'
