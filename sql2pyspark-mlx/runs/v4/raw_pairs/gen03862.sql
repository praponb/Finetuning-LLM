WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END is_remote_label,
        SUBSTRING(full_name, 1, 2) full_name_prefix,
        COALESCE(level, 'NONE') level_clean
    FROM employees
    WHERE hire_date >= DATE '2023-09-02'
      AND manager_id IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT e.*, d.email
    FROM base e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        is_remote_label,
        full_name_prefix,
        level_clean,
        email,
        ROW_NUMBER() OVER w_ordered event_seq,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_level,
        FIRST_VALUE(level, TRUE) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_level
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY dept_id, job_title ORDER BY hire_date, id)
),
enriched AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.location,
        d.dept_name,
        d.budget
    FROM with_windows s
    LEFT JOIN departments d
        ON s.dept_id = d.id
)
-- Multi-level aggregation
SELECT
    manager_id,
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    AVG(event_seq) FILTER (WHERE level = 'L1') AS l1_avg_event_seq,
    ROUND(AVG(budget), 2) AS avg_budget,
    SUM(event_seq) AS total_event_seq,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count
FROM enriched
GROUP BY manager_id, DATE_TRUNC('YEAR', hire_date)
ORDER BY year_start DESC NULLS FIRST
