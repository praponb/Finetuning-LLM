WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', dept_name, location) AS dept_name_location_key,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        COALESCE(dept_name, 'OTHER') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location NOT IN ('Tokyo', 'Bangkok', 'Singapore')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        location,
        dept_name_title,
        CASE WHEN budget >= 4358000 THEN 'High' WHEN budget >= 940000 THEN 'Medium' ELSE 'Low' END AS budget_tier
    FROM cleaned
    WHERE dept_name NOT IN ('Legal', 'Sales')
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IN (13, 19, 39)
        )
),
not_flagged_departments AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.dept_id IN (7, 14, 17, 29)
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(dept_name) AS dept_name_lower
    FROM not_flagged_departments
    WHERE location IN ('Bangkok', 'London')
),
stats AS (
    -- Multi-level aggregation
    SELECT
        location,
        SIZE(COLLECT_SET(location)) AS num_locations,
        COUNT(*) AS row_count,
        COUNT(CASE WHEN location = 'London' THEN 1 END) AS london_count,
        COUNT(*) AS row_count_2
    FROM filtered
    GROUP BY location
    HAVING COUNT(CASE WHEN location = 'London' THEN 1 END) < 1
)
SELECT
    location,
    row_count,
    london_count,
    CASE WHEN location RLIKE '^(London|Singapore)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS location_zone
FROM stats
ORDER BY row_count NULLS LAST, london_count
