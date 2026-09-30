WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(location) AS clean_location
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name IN ('Sales', 'Legal', 'Engineering')
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level = 'L1'
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(location, 'N/A') AS location_filled,
        FLOOR(budget) AS budget_whole,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM kept
    WHERE location IS NOT NULL
),
filtered_departments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        location,
        budget,
        clean_location,
        location_filled,
        budget_whole,
        dept_name_filled,
        ROUND(budget * 0.85, 2) AS adjusted_budget,
        budget * budget_whole AS budget_times_budget_whole,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM filtered
    WHERE budget >= 2075000
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget_times_budget_whole DESC, id) AS budget_times_budget_whole_bucket,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY adjusted_budget DESC, id) AS row_seq,
        LAG(budget_times_budget_whole, 1) OVER (ORDER BY budget_times_budget_whole, id) AS prev_budget_times_budget_whole
    FROM filtered_departments
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(budget_times_budget_whole_bucket) OVER (ORDER BY adjusted_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_times_budget_whole_bucket,
        FIRST_VALUE(budget_times_budget_whole, TRUE) OVER (PARTITION BY location ORDER BY budget_times_budget_whole_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget_times_budget_whole
    FROM with_windows
)
-- Ranking and gap features
SELECT
    id,
    location,
    budget,
    clean_location,
    location_filled,
    budget_whole,
    dept_name_filled,
    adjusted_budget,
    budget_times_budget_whole,
    resolved_location,
    budget_times_budget_whole_bucket,
    row_seq,
    prev_budget_times_budget_whole,
    cumulative_budget_times_budget_whole_bucket,
    first_known_budget_times_budget_whole,
    LAG(cumulative_budget_times_budget_whole_bucket, 1) OVER w_grp AS prev_cumulative_budget_times_budget_whole_bucket,
    ROW_NUMBER() OVER (ORDER BY budget, id) AS seq_no
FROM enriched
WINDOW w_grp AS (PARTITION BY location ORDER BY prev_budget_times_budget_whole, id);
