WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        budget,
        MAX(budget) OVER w_recent AS running_max_budget,
        COUNT(budget) OVER w_recent AS running_count_budget,
        SUM(budget) OVER w_recent AS running_sum_budget,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget, id) AS budget_bucket
    FROM departments
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(budget_bucket, running_max_budget) AS greatest_budget_bucket_running_max_budget
    FROM enriched
    WHERE budget > 2654000
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_sum_budget) OVER w_running AS running_sum_budget_avg_6,
        SUM(running_max_budget) OVER w_recent AS cumulative_running_max_budget
    FROM valid
    WHERE budget_bucket IS NOT NULL
    WINDOW w_running AS (ORDER BY running_sum_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_departments AS (
    -- Ranking and gap features
    SELECT
        id,
        running_count_budget,
        running_sum_budget,
        running_sum_budget_avg_6,
        SUM(cumulative_running_max_budget) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_running_max_budget,
        ROW_NUMBER() OVER w_hist AS seq_no,
        DENSE_RANK() OVER (ORDER BY running_sum_budget_avg_6 DESC) AS running_sum_budget_avg_6_dense_rank
    FROM with_windows
    WINDOW w_hist AS (ORDER BY running_max_budget DESC, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (17, 38)
        )
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        running_count_budget,
        running_sum_budget,
        running_sum_budget_avg_6,
        seq_no,
        running_sum_budget_avg_6_dense_rank,
        cumulative_cumulative_running_max_budget + running_sum_budget_avg_6_dense_rank AS cumulative_cumulative_running_max_budget_plus_running_sum_budget_avg_6_dense_rank,
        ROUND(cumulative_cumulative_running_max_budget * 100, 2) AS adjusted_cumulative_cumulative_running_max_budget
    FROM kept
    WHERE running_sum_budget_avg_6 >= 7742864
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        PERCENTILE_APPROX(running_sum_budget, 0.95) AS running_sum_budget_p95,
        AVG(running_sum_budget_avg_6) AS avg_running_sum_budget_avg_6,
        ROUND(AVG(adjusted_cumulative_cumulative_running_max_budget), 2) AS avg_adjusted_cumulative_cumulative_running_max_budget,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM base
    GROUP BY seq_no
    HAVING distinct_ids_2 > 1
)
SELECT
    seq_no,
    running_sum_budget_p95,
    avg_adjusted_cumulative_cumulative_running_max_budget,
    distinct_ids,
    distinct_ids_2
FROM summary
ORDER BY distinct_ids DESC NULLS LAST, avg_adjusted_cumulative_cumulative_running_max_budget DESC
