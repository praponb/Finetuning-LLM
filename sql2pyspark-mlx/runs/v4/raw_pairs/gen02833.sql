WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(budget, 0) AS budget_filled,
        SUBSTRING(location, 1, 4) AS location_prefix
    FROM departments
    WHERE location != 'Singapore'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id = 26
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location, dept_name ORDER BY budget) AS row_num
        FROM kept
    ) t
    WHERE row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        budget_filled,
        location_prefix,
        row_num,
        ROUND(budget * 1.1, 2) AS adjusted_budget
    FROM latest
    WHERE dept_name IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY adjusted_budget, id) AS adjusted_budget_bucket,
        ROW_NUMBER() OVER (ORDER BY row_num, id) AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY adjusted_budget) AS adjusted_budget_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_filled DESC, id) AS row_seq,
        SUM(budget_filled) OVER (PARTITION BY dept_name, location ORDER BY budget_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_filled
    FROM base
)
SELECT
    id,
    dept_name,
    location,
    budget_filled,
    location_prefix,
    adjusted_budget,
    adjusted_budget_bucket,
    event_seq,
    row_seq,
    CASE WHEN location RLIKE '^(Tokyo|Bangkok)$' THEN 'CORE' WHEN location RLIKE '^(Chiang Mai|New York)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS location_zone
FROM with_windows;
