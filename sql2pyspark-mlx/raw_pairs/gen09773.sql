WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior,
        NTILE(3) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS budget_bucket,
        SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget,
        LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget
    FROM departments
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(budget_bucket, 0) AS budget_bucket_rounded
    FROM metrics
    WHERE dept_name IS NOT NULL
),
metrics_departments AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(dept_name_sum_budget) OVER w_recent AS next_dept_name_sum_budget,
        LEAD(budget) OVER w_recent AS next_budget,
        LAG(budget) OVER w_recent AS prev_budget_2,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM valid
    WHERE budget > 3889000
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget_bucket, id)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title NOT IN ('Designer', 'Director')
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        budget_bucket,
        dept_name_sum_budget,
        prev_budget,
        budget_bucket_rounded,
        next_dept_name_sum_budget,
        next_budget,
        ROUND(budget_avg_2_prior / SUM(budget_avg_2_prior) OVER (PARTITION BY dept_name), 4) AS budget_avg_2_prior_share,
        LAG(row_seq, 1) OVER (ORDER BY budget_bucket, id) AS prev_row_seq,
        SUM(budget_bucket_rounded) OVER (PARTITION BY dept_name ORDER BY budget_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_bucket_rounded,
        LAST_VALUE(budget, TRUE) OVER (PARTITION BY dept_name ORDER BY prev_budget_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget
    FROM not_flagged
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN last_known_budget >= 3470000 THEN 'High' WHEN last_known_budget >= 1386000 THEN 'Medium' ELSE 'Low' END AS last_known_budget_level
    FROM with_windows
    WHERE dept_name_sum_budget <= 12292572
),
metrics_departments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_recent AS prev_budget_bucket,
        NTILE(10) OVER (ORDER BY budget, id) AS budget_bucket_2,
        LAST_VALUE(last_known_budget) IGNORE NULLS OVER (ORDER BY dept_name_sum_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_budget
    FROM base
    WINDOW w_recent AS (ORDER BY prev_budget DESC, id)
)
SELECT
    id,
    budget,
    dept_name_sum_budget,
    prev_budget,
    budget_bucket_rounded,
    next_budget,
    prev_row_seq,
    running_budget_bucket_rounded,
    last_known_budget
FROM metrics_departments_2
ORDER BY budget_bucket_rounded NULLS FIRST, last_known_budget DESC;
