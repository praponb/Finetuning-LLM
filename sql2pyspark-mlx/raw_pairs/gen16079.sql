WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', location, dept_name) AS location_dept_name_key,
        CASE WHEN dept_name IN ('HR', 'Finance') THEN 'Group A' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE dept_name IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget ASC NULLS LAST, id ASC) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_3_prior,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget ASC, id ASC) AS budget_bucket
    FROM ranked
    WHERE budget <= 3623000
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(budget_avg_3_prior, 0) AS budget_avg_3_prior_filled
    FROM with_windows
    WHERE budget > 1110000
),
kept_keys AS (
    -- Set difference on keys
    SELECT dept_name, location
    FROM cleaned
    EXCEPT DISTINCT
    SELECT dept_name, location
    FROM departments
    WHERE dept_name NOT IN ('Sales', 'Engineering')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.dept_name = k.dept_name AND s.location = k.location
)
SELECT
    id,
    dept_name,
    location,
    location_dept_name_key,
    dept_name_group,
    dedup_rank,
    budget_avg_3_prior,
    budget_bucket,
    budget_avg_3_prior_filled,
    CASE WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'GROUP_B' WHEN dept_name RLIKE '^(Sales|HR)$' THEN 'CORE' ELSE 'PRIORITY' END AS dept_name_zone
FROM verified
