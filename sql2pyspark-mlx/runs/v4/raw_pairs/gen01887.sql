SELECT
    id,
    email,
    DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
    INITCAP(TRIM(level)) AS level_title
FROM employees
WHERE is_remote = TRUE;
