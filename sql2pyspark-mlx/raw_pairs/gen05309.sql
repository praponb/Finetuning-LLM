WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CONCAT(level, '/', full_name) AS level_full_name,
        DATE_TRUNC('YEAR', hire_date) AS hire_date_period
    FROM employees
    WHERE hire_date >= TO_DATE('2023-08-30')
)
-- Grouped metrics
SELECT
    manager_id,
    DATE_TRUNC('YEAR', hire_date_period) AS year_start,
    DATE_TRUNC('WEEK', hire_date_period) AS week_start,
    AVG(salary) FILTER (WHERE level = 'L3') AS l3_avg_salary,
    COUNT(DISTINCT job_title) AS distinct_job_titles,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    ROUND(AVG(salary), 2) AS avg_salary,
    COUNT(DISTINCT job_title) AS distinct_job_titles_2,
    GROUPING(manager_id) AS grouping_level
FROM base
GROUP BY CUBE (manager_id, DATE_TRUNC('YEAR', hire_date_period), DATE_TRUNC('WEEK', hire_date_period))
