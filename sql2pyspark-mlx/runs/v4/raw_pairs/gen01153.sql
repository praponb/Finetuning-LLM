WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(level)) level_title
    FROM employees
    WHERE salary < 99000
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(salary) OVER (PARTITION BY manager_id) manager_id_count_salary,
        PERCENT_RANK() OVER w_running salary_percent_rank,
        AVG(salary) OVER w_recent dept_id_avg_salary
    FROM base
    WINDOW w_running AS (PARTITION BY job_title ORDER BY salary DESC), w_recent AS (PARTITION BY dept_id, manager_id)
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(dept_id_avg_salary, 1) OVER (PARTITION BY job_title ORDER BY dept_id_avg_salary, id) prev_dept_id_avg_salary,
        LAST_VALUE(manager_id_count_salary, TRUE) OVER w_part last_known_manager_id_count_salary
    FROM metrics
    WINDOW w_part AS (ORDER BY salary_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(prev_dept_id_avg_salary) IGNORE NULLS OVER (PARTITION BY manager_id ORDER BY salary_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_prev_dept_id_avg_salary,
        NTILE(4) OVER (ORDER BY salary DESC, id) salary_bucket,
        SUM(salary_percent_rank) OVER (PARTITION BY job_title ORDER BY manager_id_count_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_salary_percent_rank,
        ROW_NUMBER() OVER (ORDER BY manager_id_count_salary DESC, id) event_seq,
        NTILE(10) OVER (PARTITION BY job_title ORDER BY prev_dept_id_avg_salary, id) prev_dept_id_avg_salary_bucket
    FROM metrics_employees
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_salary_percent_rank >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_salary_percent_rank_tier,
    dept_id,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    SUM(CASE WHEN level = 'L3' THEN event_seq ELSE 0 END) AS l3_event_seq,
    ROUND(SUM(dept_id_avg_salary) / NULLIF(COUNT(CASE WHEN job_title = 'Director' THEN 1 END), 0), 2) AS dept_id_avg_salary_per_director,
    GROUPING_ID() AS grouping_id
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN cumulative_salary_percent_rank >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END, dept_id, DATE_TRUNC('MONTH', hire_date)), (CASE WHEN cumulative_salary_percent_rank >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END, dept_id), (CASE WHEN cumulative_salary_percent_rank >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END), ());
