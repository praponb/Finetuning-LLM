SELECT
    id,
    dept_id,
    full_name,
    CONCAT_WS('-', full_name, email) AS full_name_email_key,
    SUBSTRING(full_name, 1, 2) AS full_name_prefix
FROM employees
WHERE dept_id = 16 AND is_remote = TRUE
