WITH valid AS (
    SELECT *, CONCAT_WS('-', location, dept_name) AS location_dept_name_key
    FROM departments
    WHERE dept_name = 'Legal'
),
matched_keys AS (
    SELECT location
    FROM valid
    EXCEPT DISTINCT
    SELECT location
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'HR', 'Sales')
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.location = k.location
),
cleaned AS (
    SELECT
        location,
        budget,
        location_dept_name_key,
        CASE WHEN budget >= 3786000 THEN 'High' WHEN budget >= 1752000 THEN 'Medium' ELSE 'Low' END AS budget_tier,
        CAST(budget AS INT) AS budget_int,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM kept_rows
    WHERE dept_name IS NOT NULL
      AND location IS NULL
)
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS budget_cume_dist,
    ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
    MIN(budget) OVER (PARTITION BY location) AS location_min_budget
FROM cleaned
