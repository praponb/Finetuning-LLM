WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(dept_name) dept_name_lower
    FROM departments
    WHERE location IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.dept_name_lower,
        latest.job_title,
        latest.employee_hire_date
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, job_title ORDER BY employee_hire_date, id) = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.level,
        latest.employee_job_title
    FROM ranked s
    JOIN LATERAL (
            SELECT c.level, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', employee_hire_date) AS year_start,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END) AS engineer_count
FROM with_last
GROUP BY year_start
ORDER BY year_start DESC NULLS LAST;
