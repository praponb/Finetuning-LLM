SELECT
    id,
    level,
    salary,
    IF(is_remote, 'Yes', 'No') AS is_remote_label,
    SUBSTRING(email, 1, 3) AS email_prefix
FROM employees
WHERE UPPER(email) LIKE '%ANY%';
