SELECT
    level,
    is_remote,
    email,
    UPPER(full_name) AS full_name_upper
FROM employees
WHERE email LIKE '%@startup.io'
ORDER BY level DESC;
