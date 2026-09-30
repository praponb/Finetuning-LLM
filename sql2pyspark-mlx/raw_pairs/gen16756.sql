WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', location, dept_name) AS location_dept_name_key
    FROM departments
    WHERE location IS NOT DISTINCT FROM 'Tokyo'
)
-- Aggregate with conditional metrics
SELECT
    location,
    PERCENTILE_APPROX(budget, 0.9) AS budget_p90,
    COUNT(*) FILTER (WHERE dept_name = 'Engineering') AS engineering_rows,
    COUNT(CASE WHEN location = 'Bangkok' THEN 1 END) AS bangkok_count
FROM cleaned
GROUP BY location
ORDER BY engineering_rows DESC
