WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        level,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
        CEIL(salary) AS salary_whole,
        COALESCE(level, 'OTHER') AS level_clean
    FROM employees
    WHERE job_title IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(salary_whole) OVER (PARTITION BY level ORDER BY salary_whole, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_whole_avg_5,
        AVG(salary_whole) OVER (PARTITION BY manager_id, dept_id ORDER BY days_since_hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS salary_whole_avg_6,
        LAST_VALUE(days_since_hire_date) IGNORE NULLS OVER (ORDER BY salary_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_hire_date
    FROM base
    WHERE salary_whole > 50
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        manager_id,
        level,
        days_since_hire_date,
        salary_whole,
        level_clean,
        last_known_days_since_hire_date,
        AVG(salary_whole_avg_6) OVER (PARTITION BY level ORDER BY salary_whole_avg_5, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_whole_avg_6_avg_2,
        NTILE(5) OVER (PARTITION BY level ORDER BY days_since_hire_date, id) AS days_since_hire_date_bucket
    FROM scored
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary_whole_avg_6_avg_2) OVER w_running AS cumulative_salary_whole_avg_6_avg_2,
        AVG(salary_whole_avg_6_avg_2) OVER (PARTITION BY level, manager_id ORDER BY days_since_hire_date_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_whole_avg_6_avg_2_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY level, manager_id ORDER BY days_since_hire_date_bucket DESC) AS days_since_hire_date_bucket_percent_rank,
        AVG(days_since_hire_date_bucket) OVER (PARTITION BY level ORDER BY days_since_hire_date_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_hire_date_bucket_avg_6,
        RANK() OVER (ORDER BY last_known_days_since_hire_date DESC) AS last_known_days_since_hire_date_rank
    FROM scored_employees
    WINDOW w_running AS (PARTITION BY level ORDER BY days_since_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
totals AS (
    -- Grouped metrics
    SELECT
        manager_id,
        COUNT_IF(level = 'L1') AS l1_events,
        ROUND(AVG(days_since_hire_date_bucket_percent_rank), 2) AS avg_days_since_hire_date_bucket_percent_rank,
        ROUND(AVG(days_since_hire_date_bucket), 2) AS avg_days_since_hire_date_bucket,
        COUNT(*) FILTER (WHERE level = 'L4') AS l4_rows,
        AVG(salary_whole_avg_6_avg_2) AS avg_salary_whole_avg_6_avg_2
    FROM enriched
    GROUP BY manager_id
)
SELECT
    manager_id,
    l1_events,
    avg_days_since_hire_date_bucket_percent_rank,
    avg_salary_whole_avg_6_avg_2
FROM totals
ORDER BY avg_days_since_hire_date_bucket_percent_rank DESC NULLS LAST
