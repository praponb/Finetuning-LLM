WITH base AS (
    SELECT *, TRIM(dept_name) clean_dept_name
    FROM departments
    WHERE dept_name NOT IN ('Engineering', 'Sales', 'Finance') AND dept_name = 'Legal'
),
summary AS (
    SELECT dept_name, location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) location_values
    FROM base
    GROUP BY dept_name, location
)
SELECT *
FROM summary
ORDER BY location_values, dept_name, location
LIMIT 50;
