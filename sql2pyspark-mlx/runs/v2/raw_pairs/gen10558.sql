SELECT
    email,
    manager_id,
    is_remote,
    INITCAP(TRIM(email)) AS email_title
FROM employees
ORDER BY email DESC
