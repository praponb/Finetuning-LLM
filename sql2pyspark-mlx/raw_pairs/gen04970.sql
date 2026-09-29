SELECT
    dept_id,
    full_name,
    manager_id,
    LOWER(email) email_lower
FROM employees
WHERE NOT (dept_id = 31 AND job_title != 'Engineer' AND hire_date > '2024-05-01');
