WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS seq_no,
        SUM(budget) OVER w_running AS dept_name_sum_budget,
        LEAD(budget, 1) OVER (ORDER BY budget, id) AS next_budget
    FROM departments
    WINDOW w_running AS (PARTITION BY dept_name)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_bucket,
        seq_no,
        dept_name_sum_budget,
        AVG(seq_no) OVER (PARTITION BY dept_name) AS dept_name_avg_seq_no,
        SUM(next_budget) OVER w_hist AS running_next_budget
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER w_recent AS budget_bucket_bucket,
        NTILE(5) OVER w_ordered AS running_next_budget_bucket
    FROM metrics
    WINDOW w_recent AS (PARTITION BY location, dept_name ORDER BY budget_bucket, id), w_ordered AS (ORDER BY running_next_budget DESC, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY location, dept_name ORDER BY running_next_budget_bucket NULLS LAST) AS running_next_budget_bucket_dense_rank,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY running_next_budget DESC) AS running_next_budget_dense_rank,
        LAST_VALUE(location) IGNORE NULLS OVER w_recent AS last_known_location,
        ROUND(running_next_budget_bucket / SUM(running_next_budget_bucket) OVER (PARTITION BY dept_name), 4) AS running_next_budget_bucket_share
    FROM enriched
    WINDOW w_recent AS (PARTITION BY location ORDER BY dept_name_avg_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_job_title,
        latest.employee_level
    FROM scored s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    dept_name,
    employee_job_title,
    COUNT_IF(dept_name = 'Sales') AS sales_events,
    SIZE(COLLECT_SET(employee_level)) AS num_employee_levels,
    SUM(running_next_budget_bucket) FILTER (WHERE location = 'London') AS london_total_running_next_budget_bucket,
    COUNT(*) FILTER (WHERE employee_job_title = 'Analyst') AS analyst_rows
FROM with_latest
GROUP BY CUBE (dept_name, employee_job_title)
