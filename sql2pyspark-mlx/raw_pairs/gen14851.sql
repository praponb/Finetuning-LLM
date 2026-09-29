WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_hire_date
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(budget) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS seq_no,
        NTILE(10) OVER (ORDER BY budget, id) AS budget_bucket
    FROM latest_match
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        employee_hire_date,
        budget_bucket
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_bucket DESC NULLS LAST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        employee_hire_date,
        budget_bucket,
        LAG(budget_bucket) OVER (ORDER BY employee_hire_date, id) AS prev_budget_bucket,
        LEAD(budget_bucket) OVER (ORDER BY employee_hire_date, id) AS next_budget_bucket,
        ROW_NUMBER() OVER (ORDER BY employee_hire_date, id) AS event_seq
    FROM ranked
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget_bucket,
        s.prev_budget_bucket,
        s.event_seq,
        top1.hire_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_budget_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_bucket_tier,
        CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
        CASE WHEN budget_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS budget_bucket_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2,
        ROUND(AVG(budget_bucket), 2) AS avg_budget_bucket,
        PERCENTILE_APPROX(budget_bucket, 0.95) AS budget_bucket_p95
    FROM with_latest
    GROUP BY ROLLUP (CASE WHEN prev_budget_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN budget_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    budget_bucket_tier,
    distinct_ids,
    distinct_ids_2
FROM grouped;
