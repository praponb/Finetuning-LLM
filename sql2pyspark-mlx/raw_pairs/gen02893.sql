WITH filtered AS (
    SELECT *, SUBSTRING(location, 1, 4) AS location_prefix
    FROM departments
    WHERE budget < 1164000
),
with_last AS (
    SELECT
        s.id,
        s.location,
        s.budget,
        s.location_prefix,
        x.employee_level
    FROM filtered s
    JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IN ('L1', 'L5', 'L2')
        )
),
scored AS (
    SELECT
        id,
        location_prefix,
        NTILE(10) OVER (PARTITION BY employee_level ORDER BY budget DESC, id) AS budget_bucket,
        NTILE(5) OVER (PARTITION BY employee_level ORDER BY budget, id) AS budget_bucket_2
    FROM not_flagged
),
with_latest AS (
    SELECT
        s.*,
        top1.employee_job_title,
        top1.employee_level
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        id,
        location_prefix,
        budget_bucket,
        budget_bucket_2,
        employee_job_title,
        employee_level,
        SUM(budget_bucket) OVER (PARTITION BY employee_level ORDER BY budget_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget_bucket,
        MIN(budget_bucket) OVER (PARTITION BY employee_level ORDER BY budget_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget_bucket,
        COUNT(budget_bucket) OVER (PARTITION BY employee_level ORDER BY budget_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget_bucket,
        AVG(budget_bucket) OVER (PARTITION BY employee_level ORDER BY budget_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget_bucket,
        DENSE_RANK() OVER (PARTITION BY employee_level, employee_job_title ORDER BY budget_bucket DESC) AS budget_bucket_dense_rank
    FROM with_latest
)
SELECT
    id,
    location_prefix,
    budget_bucket_2,
    employee_job_title,
    employee_level,
    running_sum_budget_bucket,
    running_min_budget_bucket,
    running_count_budget_bucket,
    budget_bucket_dense_rank
FROM metrics
ORDER BY running_count_budget_bucket
