SELECT
    id,
    is_remote,
    email,
    LOWER(level) AS level_lower
FROM employees
