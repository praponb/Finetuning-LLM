SELECT
    id,
    manager_id,
    level,
    INITCAP(TRIM(full_name)) AS full_name_title,
    UPPER(level) AS level_upper
FROM employees
ORDER BY full_name_title;
