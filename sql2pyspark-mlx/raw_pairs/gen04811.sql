SELECT
    job_title,
    email,
    dept_id,
    CONCAT(email, '/', full_name) AS email_full_name
FROM employees
WHERE dept_id IN (17, 28, 37);
