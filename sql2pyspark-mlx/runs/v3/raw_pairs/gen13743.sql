SELECT
    id,
    email,
    level,
    salary,
    IF(is_remote, 'Yes', 'No') is_remote_label
FROM employees
ORDER BY id DESC;
