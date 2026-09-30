WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        latest.hire_date,
        latest.level
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.hire_date, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        level,
        ROUND(budget / SUM(budget) OVER (PARTITION BY level), 4) AS budget_share,
        LAST_VALUE(budget) IGNORE NULLS OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget,
        LAG(budget, 1) OVER (PARTITION BY level ORDER BY budget, id) AS prev_budget
    FROM latest_match
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.level,
        s.budget_share,
        x.hire_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN budget_share >= 0.75 THEN 'LARGE' ELSE 'SMALL' END AS budget_share_tier,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(budget_share, 0.95) AS budget_share_p95,
        COUNT_IF(level = 'L3') AS l3_events,
        COUNT_IF(level = 'L4') AS l4_events,
        AVG(budget_share) AS avg_budget_share,
        ROUND(SUM(budget_share) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) AS budget_share_per_l1
    FROM with_latest
    GROUP BY CASE WHEN budget_share >= 0.75 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    budget_share_tier,
    budget_share_p95,
    l3_events,
    avg_budget_share
FROM totals
