WITH filtered AS (
    SELECT
        *,
        ROUND(budget * 100, 2) budget_adjusted,
        CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END is_sales,
        COALESCE(dept_name, 'DIRECT') dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IN (5, 8, 16, 37)
        )
),
base AS (
    SELECT *, LENGTH(location) location_length
    FROM eligible
    WHERE budget_adjusted IS NOT NULL
),
valid AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_filled,
        location_length,
        TRIM(location) clean_location
    FROM base
    WHERE dept_name IS NOT NULL
      AND dept_name = 'Sales'
),
summary AS (
    SELECT
        location,
        SUM(CASE WHEN location = 'London' THEN location_length ELSE 0 END) AS london_location_length,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(location)) AS num_locations,
        SORT_ARRAY(COLLECT_SET(location)) AS location_list,
        ROUND(AVG(location_length), 2) AS avg_location_length
    FROM valid
    GROUP BY 1
)
SELECT
    location,
    num_rows,
    location_list,
    avg_location_length,
    CASE WHEN location RLIKE '^(Singapore|Chiang Mai)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS location_zone
FROM summary
