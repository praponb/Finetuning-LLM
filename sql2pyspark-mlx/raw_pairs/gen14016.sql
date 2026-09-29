WITH enriched AS (
    -- Attach employees attributes
    SELECT s.*, p.hire_date AS employee_hire_date
    FROM employees s
    LEFT JOIN employees p
        ON s.manager_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY level ORDER BY salary DESC, id ASC) AS salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY manager_id ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM enriched
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary_share) OVER (PARTITION BY job_title) AS job_title_avg_salary_share,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', employee_hire_date), manager_id ORDER BY salary ASC, id ASC) AS row_seq,
        NTILE(10) OVER w_recent AS salary_bucket_bucket
    FROM metrics
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY salary_bucket DESC, id ASC)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary_bucket_bucket DESC, id ASC) = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', employee_hire_date) AS quarter_start,
    COUNT(*) AS num_rows,
    SUM(job_title_avg_salary_share) FILTER (WHERE level = 'L5') AS l5_total_job_title_avg_salary_share
FROM deduped
GROUP BY 1
