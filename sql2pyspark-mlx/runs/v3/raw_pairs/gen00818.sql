SELECT
    level,
    full_name,
    dept_id,
    UPPER(level) AS level_upper,
    CONCAT_WS('-', full_name, level) AS full_name_level_key
FROM employees
WHERE is_remote = TRUE OR level <> 'L5';
