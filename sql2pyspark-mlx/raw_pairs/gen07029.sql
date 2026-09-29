WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST, id ASC) AS latest_rank
        FROM departments
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        latest_rank,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY latest_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget,
        MIN(budget) OVER (PARTITION BY dept_name ORDER BY latest_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget,
        MAX(budget) OVER (PARTITION BY dept_name ORDER BY latest_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share
    FROM latest
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, running_min_budget * budget AS running_min_budget_times_budget
    FROM metrics
    WHERE running_min_budget > 2051000
),
metrics_departments AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(running_max_budget) OVER w_grp AS running_count_running_max_budget,
        AVG(running_max_budget) OVER w_grp AS running_avg_running_max_budget,
        SUM(running_max_budget) OVER w_grp AS running_sum_running_max_budget,
        MIN(running_max_budget) OVER w_grp AS running_min_running_max_budget
    FROM filtered
    WINDOW w_grp AS (ORDER BY running_min_budget_times_budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.running_avg_budget,
        s.running_max_budget,
        s.budget_share,
        s.running_min_budget_times_budget,
        s.running_count_running_max_budget,
        s.running_avg_running_max_budget,
        s.running_sum_running_max_budget,
        top1.employee_job_title,
        top1.employee_hire_date
    FROM metrics_departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', employee_hire_date) AS quarter_start,
    COUNT(*) FILTER (WHERE employee_job_title = 'Intern') AS intern_rows,
    COUNT_IF(employee_job_title = 'Engineer') AS engineer_events,
    MAX(budget_share) AS max_budget_share
FROM with_latest
GROUP BY DATE_TRUNC('QUARTER', employee_hire_date)
ORDER BY intern_rows DESC NULLS FIRST;
