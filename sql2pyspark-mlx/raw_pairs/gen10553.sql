WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', location, dept_name) AS location_dept_name_key,
        TRIM(location) AS clean_location
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget >= 1252000
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        budget,
        clean_location,
        LEAD(budget, 1, 0) OVER (PARTITION BY dept_name ORDER BY budget, id) AS next_budget,
        LAST_VALUE(dept_name, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        NTILE(3) OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS budget_bucket
    FROM base
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.level = 'L5'
        )
),
kept_departments AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary >= 140000
        )
),
base_departments AS (
    -- Basic cleansing and defaults
    SELECT *, budget_bucket * next_budget AS budget_bucket_times_next_budget
    FROM kept_departments
    WHERE budget_bucket IS NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        budget,
        clean_location,
        last_known_dept_name,
        budget_bucket,
        budget_bucket_times_next_budget,
        SUM(budget_bucket_times_next_budget) OVER (ORDER BY budget_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget_bucket_times_next_budget,
        MIN(budget_bucket_times_next_budget) OVER (ORDER BY budget_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget_bucket_times_next_budget,
        MAX(budget_bucket_times_next_budget) OVER (ORDER BY budget_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget_bucket_times_next_budget
    FROM base_departments
),
metrics_departments AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(budget) OVER w_running AS running_min_budget,
        MAX(budget) OVER w_running AS running_max_budget,
        AVG(budget) OVER w_running AS running_avg_budget,
        SUM(running_min_budget_bucket_times_next_budget) OVER (ORDER BY running_sum_budget_bucket_times_next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_min_budget_bucket_times_next_budget
    FROM metrics
    WINDOW w_running AS (ORDER BY budget_bucket_times_next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        budget,
        running_max_budget
    FROM metrics_departments
) src
UNPIVOT (
    reading FOR measure IN (budget AS BUDGET, running_max_budget AS RUNNING_MAX_BUDGET_VAL)
);
