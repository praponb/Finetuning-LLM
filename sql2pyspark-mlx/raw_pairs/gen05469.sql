WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        CAST(is_remote AS INT) AS is_remote_flag,
        SUBSTRING(full_name, 1, 3) AS full_name_prefix,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM employees
    WHERE hire_date BETWEEN '2024-01-11' AND '2024-08-01'
)
-- Join a dimension and aggregate
SELECT
    p.level,
    s.job_title AS employee_job_title,
    COUNT(*) AS cnt,
    ROUND(SUM(s.is_remote_flag), 2) AS total_is_remote_flag,
    GROUPING_ID() AS level_id
FROM base s
JOIN employees p ON s.manager_id = p.id
GROUP BY GROUPING SETS ((p.level, s.job_title), (p.level), ());
