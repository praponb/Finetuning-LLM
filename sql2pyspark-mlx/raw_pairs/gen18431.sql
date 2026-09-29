WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(dept_name) AS clean_dept_name,
        ROUND(budget * 1.15, 2) AS adjusted_budget
    FROM departments
    WHERE budget BETWEEN 1562000 AND 2632000
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id IS NULL
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY adjusted_budget DESC, id) AS row_num
    FROM eligible
    QUALIFY row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(dept_name, '/', location) AS dept_name_location
    FROM ranked
    WHERE location IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        location,
        budget,
        dept_name_location,
        ROUND(adjusted_budget, 2) AS adjusted_budget_rounded,
        SUBSTRING(location, 1, 3) AS location_prefix,
        COALESCE(location, 'OTHER') AS location_filled
    FROM filtered
    WHERE location IS DISTINCT FROM 'Chiang Mai'
)
-- Running and moving aggregates
SELECT
    id,
    location,
    budget,
    dept_name_location,
    adjusted_budget_rounded,
    location_prefix,
    COUNT(adjusted_budget_rounded) OVER (PARTITION BY location) AS location_count_adjusted_budget_rounded,
    SUM(adjusted_budget_rounded) OVER w_part AS cumulative_adjusted_budget_rounded,
    AVG(budget) OVER w_part_2 AS budget_avg_6
FROM valid
WHERE budget BETWEEN 2530000 AND 3218000
WINDOW w_part AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY adjusted_budget_rounded, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
