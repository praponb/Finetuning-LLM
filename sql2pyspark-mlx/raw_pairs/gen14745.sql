WITH base AS (
    SELECT *, CASE WHEN location IN ('Bangkok', 'Tokyo') THEN 'Core' ELSE 'Other' END AS location_group
    FROM departments
    WHERE location IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND LOWER(c.email) LIKE '%vy@%'
        )
),
valid AS (
    SELECT
        id,
        budget,
        CAST(budget AS INT) AS budget_int,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM not_flagged
    WHERE location <> 'New York'
),
with_windows AS (
    SELECT
        *,
        SUM(budget_int) OVER w_grp AS running_budget_int,
        ROW_NUMBER() OVER (ORDER BY budget_int DESC, id) AS seq_no,
        SUM(budget) OVER w_grp AS running_budget,
        AVG(budget) OVER (ORDER BY budget_int, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior
    FROM valid
    WINDOW w_grp AS (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        LEAD(seq_no) OVER (ORDER BY running_budget_int, id) AS next_seq_no,
        LAG(running_budget_int) OVER (ORDER BY running_budget_int, id) AS prev_running_budget_int,
        LEAD(running_budget_int) OVER (ORDER BY running_budget_int, id) AS next_running_budget_int,
        ROW_NUMBER() OVER (ORDER BY running_budget_int, id) AS seq_no_2,
        SUM(budget_int) OVER (ORDER BY running_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_int
    FROM with_windows
)
SELECT
    seq_no,
    next_seq_no,
    ROUND(AVG(budget), 2) AS avg_budget,
    APPROX_PERCENTILE(running_budget_int, ARRAY(0.25, 0.5, 0.75)) AS running_budget_int_quartiles,
    APPROX_PERCENTILE(seq_no_2, 0.95) AS seq_no_2_p95,
    APPROX_PERCENTILE(cumulative_budget_int, 0.5) AS cumulative_budget_int_p50
FROM metrics
GROUP BY seq_no, next_seq_no
HAVING seq_no_2_p95 < 1000000;
