WITH base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(dept_name) AS dept_name_upper
    FROM departments
    WHERE location IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        dept_name_upper,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, dept_name), 4) AS budget_share,
        FIRST_VALUE(budget, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY dept_name, location ORDER BY budget NULLS LAST) AS budget_cume_dist,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM base
    WHERE location IN ('London', 'Tokyo', 'Bangkok')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        budget_share,
        first_known_budget,
        row_seq,
        budget_cume_dist,
        LAG(budget_share, 1) OVER (ORDER BY budget_share, id) AS prev_budget_share,
        ROW_NUMBER() OVER (ORDER BY first_known_budget DESC, id) AS event_seq,
        LAG(budget_cume_dist, 1) OVER (ORDER BY budget_cume_dist, id) AS prev_budget_cume_dist,
        AVG(budget_share) OVER (ORDER BY budget_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_share_avg_2,
        PERCENT_RANK() OVER (ORDER BY row_seq DESC NULLS FIRST) AS row_seq_percent_rank
    FROM enriched
),
enriched_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(budget_cume_dist, TRUE) OVER w_ordered AS first_known_budget_cume_dist,
        ROW_NUMBER() OVER (ORDER BY budget_share_avg_2 DESC, id) AS row_seq_2
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY budget_share_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    budget_share,
    first_known_budget,
    prev_budget_share,
    prev_budget_cume_dist,
    budget_share_avg_2,
    row_seq_percent_rank,
    first_known_budget_cume_dist,
    row_seq_2
FROM enriched_departments
ORDER BY first_known_budget_cume_dist NULLS LAST;
