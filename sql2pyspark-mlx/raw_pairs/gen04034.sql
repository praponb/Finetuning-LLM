WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        job_title,
        hire_date,
        is_remote,
        level,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM employees
    WHERE level IS NOT NULL
      AND dept_id IN (13, 22, 33)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), hire_date) / 365.25 AS years_since_hire_date,
        COALESCE(job_title, 'OTHER') AS resolved_job_title
    FROM filtered
    WHERE level IS NOT NULL
      AND LENGTH(full_name) >= 9
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        job_title,
        hire_date,
        level,
        is_remote_label,
        resolved_job_title,
        FIRST_VALUE(years_since_hire_date, TRUE) OVER (PARTITION BY job_title ORDER BY years_since_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_years_since_hire_date,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('week', hire_date), job_title ORDER BY hire_date DESC, id) AS row_seq
    FROM cleaned
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        is_remote_label,
        first_known_years_since_hire_date,
        MIN(event_seq) OVER w_part AS running_min_event_seq,
        MAX(event_seq) OVER w_part AS running_max_event_seq,
        COUNT(event_seq) OVER w_part AS running_count_event_seq
    FROM enriched
    WHERE job_title = 'Manager'
    WINDOW w_part AS (PARTITION BY level ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    level,
    SUM(running_count_event_seq) AS total_running_count_event_seq,
    APPROX_PERCENTILE(running_count_event_seq, ARRAY(0.25, 0.5, 0.75)) AS running_count_event_seq_quartiles,
    ROUND(SUM(running_count_event_seq) / NULLIF(COUNT(CASE WHEN level = 'L4' THEN 1 END), 0), 2) AS running_count_event_seq_per_l4,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched_employees
GROUP BY 1;
