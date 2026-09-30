WITH with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        latest.employee_level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        employee_level,
        ROW_NUMBER() OVER (ORDER BY budget, id) AS seq_no,
        ROUND(budget / SUM(budget) OVER (PARTITION BY employee_level), 4) AS budget_share,
        NTILE(10) OVER (PARTITION BY employee_level ORDER BY budget, id) AS budget_bucket
    FROM with_latest
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, budget_bucket - budget_share AS budget_bucket_minus_budget_share
    FROM enriched
    WHERE employee_level IS NOT NULL
      AND employee_level IN ('L4', 'L5', 'L1')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (22, 24, 27, 36)
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_hire_date
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget >= 3287000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    SIZE(COLLECT_SET(employee_level)) AS num_employee_levels,
    SUM(seq_no) AS total_seq_no,
    STDDEV_SAMP(budget) AS budget_stddev
FROM latest_match
GROUP BY CASE WHEN budget >= 3287000 THEN 'LARGE' ELSE 'SMALL' END
HAVING SIZE(COLLECT_SET(employee_level)) > 1
ORDER BY budget_tier DESC;
