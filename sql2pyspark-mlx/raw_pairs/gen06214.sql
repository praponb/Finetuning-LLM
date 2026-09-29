WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget * 1.07, 2) AS budget_scaled,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(budget, 1, 0) OVER (PARTITION BY location, dept_name ORDER BY budget_scaled, id) AS next_budget,
        PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY budget NULLS FIRST) AS budget_percent_rank
    FROM filtered
    WHERE budget > 3034000
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id) = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN budget_percent_rank >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_percent_rank_tier,
        COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count,
        SUM(next_budget) AS total_next_budget
    FROM latest
    GROUP BY CASE WHEN budget_percent_rank >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    budget_percent_rank_tier,
    new_york_count,
    total_next_budget
FROM totals
