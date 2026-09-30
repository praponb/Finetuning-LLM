WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        CONCAT(job_title, ' ', level) AS job_title_level,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-04-29'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        job_title_level,
        resolved_job_title,
        CAST(is_remote AS INT) AS is_remote_flag,
        UPPER(full_name) AS full_name_upper,
        COALESCE(job_title, 'N/A') AS resolved_job_title_2
    FROM filtered
    WHERE hire_date >= DATE '2023-03-23'
      AND job_title IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.manager_id,
        e.job_title,
        e.hire_date,
        e.resolved_job_title,
        e.is_remote_flag,
        d.email AS employee_email
    FROM valid e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        manager_id,
        hire_date,
        resolved_job_title,
        is_remote_flag,
        employee_email,
        MIN(is_remote_flag) OVER w_grp AS running_min_is_remote_flag,
        SUM(is_remote_flag) OVER w_grp AS running_sum_is_remote_flag,
        COUNT(is_remote_flag) OVER w_grp AS running_count_is_remote_flag,
        DENSE_RANK() OVER w_running AS is_remote_flag_dense_rank
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY job_title ORDER BY is_remote_flag ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY YEAR(hire_date), manager_id ORDER BY is_remote_flag DESC)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
        DATE_TRUNC('WEEK', hire_date) AS week_start,
        DATE_TRUNC('YEAR', hire_date) AS year_start,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(running_count_is_remote_flag, ARRAY(0.25, 0.5, 0.75)) AS running_count_is_remote_flag_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(running_count_is_remote_flag), 2) AS avg_running_count_is_remote_flag
    FROM metrics
    GROUP BY ROLLUP (DATE_TRUNC('QUARTER', hire_date), DATE_TRUNC('WEEK', hire_date), DATE_TRUNC('YEAR', hire_date))
    HAVING distinct_ids > 1
)
SELECT
    week_start,
    year_start,
    distinct_ids,
    avg_running_count_is_remote_flag
FROM stats
ORDER BY distinct_ids ASC NULLS FIRST
