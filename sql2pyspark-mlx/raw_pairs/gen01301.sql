WITH valid AS (
    SELECT
        *,
        CASE WHEN budget >= 3649000 THEN 'High' WHEN budget >= 1725000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        COALESCE(budget, 0) AS budget_filled
    FROM departments
    WHERE location IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS LAST, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget_filled DESC) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
metrics AS (
    SELECT
        id,
        budget_filled,
        SUM(budget_filled) OVER (PARTITION BY location ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget_filled,
        AVG(budget_filled) OVER (PARTITION BY location ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget_filled,
        COUNT(budget_filled) OVER (PARTITION BY location ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget_filled
    FROM latest
    WHERE budget < 3983000
),
with_windows AS (
    SELECT
        *,
        AVG(budget_filled) OVER w_recent AS budget_filled_avg_2,
        LEAD(running_count_budget_filled, 1) OVER (ORDER BY running_avg_budget_filled, id) AS next_running_count_budget_filled,
        LEAD(running_avg_budget_filled, 1) OVER w_part AS next_running_avg_budget_filled
    FROM metrics
    WINDOW w_recent AS (ORDER BY budget_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part AS (ORDER BY budget_filled, id)
)
SELECT
    id,
    running_avg_budget_filled,
    running_count_budget_filled,
    budget_filled_avg_2
FROM with_windows;
