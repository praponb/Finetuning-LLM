WITH with_windows AS (
    SELECT
        id,
        location,
        budget,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        DENSE_RANK() OVER w_recent AS budget_dense_rank
    FROM departments
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget DESC)
),
remaining_keys AS (
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM departments
    WHERE dept_name = 'Marketing'
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2,
        ROUND(budget_dense_rank / SUM(budget_dense_rank) OVER (PARTITION BY location), 4) AS budget_dense_rank_share,
        RANK() OVER (ORDER BY running_budget ASC) AS running_budget_rank,
        ROUND(running_budget / SUM(running_budget) OVER (PARTITION BY location), 4) AS running_budget_share
    FROM verified
    WHERE budget_dense_rank BETWEEN 3 AND 5
),
kept AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.is_remote = FALSE
        )
),
cleaned AS (
    SELECT
        location,
        budget,
        running_budget,
        budget_dense_rank,
        budget_share_2,
        running_budget_rank,
        running_budget_share,
        UPPER(location) AS location_upper
    FROM kept
    WHERE location IS NOT NULL
      AND running_budget_share IS NOT NULL
),
base AS (
    SELECT
        budget,
        running_budget,
        budget_dense_rank,
        budget_share_2,
        running_budget_rank,
        ROUND(budget_share_2 / running_budget, 2) AS budget_share_2_per_running_budget
    FROM cleaned
    WHERE location IS NOT NULL
)
SELECT
    budget,
    budget_dense_rank,
    budget_share_2,
    running_budget_rank,
    budget_share_2_per_running_budget
FROM base
ORDER BY budget_share_2 DESC
