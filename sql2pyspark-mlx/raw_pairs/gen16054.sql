WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, location
    FROM departments
    EXCEPT ALL
    SELECT id, location
    FROM departments
    WHERE location IN ('Singapore', 'Berlin', 'Bangkok')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.location = k.location
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN budget >= 3040000 THEN 'High' WHEN budget >= 1522000 THEN 'Medium' ELSE 'Low' END AS budget_level
    FROM verified
    WHERE dept_name NOT IN ('Legal', 'Sales')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, dept_name
    FROM filtered
    INTERSECT ALL
    SELECT id, dept_name
    FROM departments
    WHERE location <> 'Berlin'
),
verified_departments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id AND s.dept_name = k.dept_name
),
scored AS (
    -- Running and moving aggregates
    SELECT *, COUNT(budget) OVER (PARTITION BY dept_name) AS dept_name_count_budget
    FROM verified_departments
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(dept_name_count_budget) OVER (PARTITION BY location) AS location_max_dept_name_count_budget,
        ROUND(dept_name_count_budget / SUM(dept_name_count_budget) OVER (PARTITION BY dept_name), 4) AS dept_name_count_budget_share,
        RANK() OVER (ORDER BY budget NULLS FIRST) AS budget_rank
    FROM scored
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        dept_name,
        location,
        budget,
        budget_level,
        dept_name_count_budget,
        dept_name_count_budget_share,
        budget_rank,
        CAST(budget AS INT) AS budget_int,
        COALESCE(location, 'N/A') AS location_filled
    FROM not_flagged
    WHERE location IS NOT NULL
)
SELECT
    dept_name,
    location,
    budget,
    budget_level,
    dept_name_count_budget,
    budget_int,
    location_filled
FROM cleaned
ORDER BY budget NULLS FIRST
