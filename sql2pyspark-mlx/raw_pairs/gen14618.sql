WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN budget >= 3402000 THEN 'High' WHEN budget >= 1568000 THEN 'Medium' ELSE 'Low' END AS budget_level
    FROM departments
    WHERE location IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IN ('L1', 'L5')
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget,
        budget_level,
        MIN(budget) OVER w_recent AS running_min_budget,
        MAX(budget) OVER w_recent AS running_max_budget,
        SUM(budget) OVER w_recent AS running_sum_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(running_max_budget) OVER (PARTITION BY location ORDER BY running_min_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_max_budget,
        MIN(running_max_budget) OVER (PARTITION BY location ORDER BY running_min_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_max_budget,
        SUM(running_max_budget) OVER (PARTITION BY location ORDER BY running_min_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_max_budget
    FROM metrics
    WHERE budget IS NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_share, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
)
SELECT
    id,
    location,
    budget,
    budget_level,
    running_min_budget,
    budget_share,
    running_min_running_max_budget,
    latest_rank
FROM deduped
