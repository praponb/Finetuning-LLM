SELECT
    manager_id,
    full_name,
    INITCAP(TRIM(level)) AS level_title,
    CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
FROM employees
ORDER BY full_name DESC
