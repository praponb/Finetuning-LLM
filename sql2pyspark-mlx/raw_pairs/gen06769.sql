WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id ASC) AS dedup_rank
    FROM departments
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget ASC, id ASC) AS prev_budget,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name, location ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name
    FROM deduped
    WHERE location NOT IN ('New York', 'Tokyo', 'Berlin')
),
metrics_departments AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS LAST) AS budget_percent_rank,
        LEAD(budget, 2) OVER (PARTITION BY dept_name ORDER BY dedup_rank ASC, id ASC) AS next_budget,
        CUME_DIST() OVER (ORDER BY budget DESC NULLS FIRST) AS budget_cume_dist,
        NTILE(3) OVER (PARTITION BY dept_name, location ORDER BY prev_budget ASC, id ASC) AS prev_budget_bucket,
        AVG(budget) OVER w_hist AS budget_avg_5_prior
    FROM metrics
    WINDOW w_hist AS (PARTITION BY location ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.employee_level
    FROM metrics_departments s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level IN ('L1', 'L5', 'L4')
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        dedup_rank,
        prev_budget,
        last_known_dept_name,
        next_budget,
        prev_budget_bucket,
        NTILE(4) OVER (PARTITION BY employee_level ORDER BY budget_cume_dist DESC, id ASC) AS budget_cume_dist_bucket,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_percent_rank DESC, id ASC) AS seq_no,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget DESC, id ASC) AS budget_bucket,
        RANK() OVER (ORDER BY budget_cume_dist DESC NULLS LAST) AS budget_cume_dist_rank
    FROM eligible
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date < '2024-08-23'
        )
)
SELECT
    id,
    location,
    last_known_dept_name,
    next_budget,
    prev_budget_bucket,
    budget_cume_dist_bucket,
    seq_no,
    budget_bucket,
    budget_cume_dist_rank,
    CASE WHEN location RLIKE '^(Tokyo|New York)$' THEN 'CORE' ELSE 'GROUP_B' END AS location_zone
FROM not_flagged
ORDER BY budget_cume_dist_bucket DESC
