SELECT
    id,
    email,
    level,
    dept_id,
    SUBSTRING(level, 1, 4) AS level_prefix
FROM employees
WHERE full_name LIKE '%Pim%' AND manager_id IS NOT NULL
ORDER BY level
