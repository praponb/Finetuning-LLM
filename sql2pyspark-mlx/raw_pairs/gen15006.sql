WITH cleaned AS (
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        LOWER(location) AS location_lower,
        COALESCE(location, 'UNKNOWN') AS location_filled
    FROM departments
    WHERE dept_name NOT IN ('Marketing', 'Engineering')
)
SELECT
    location,
    SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget,
    COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows,
    COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows
FROM cleaned
GROUP BY location
ORDER BY location DESC NULLS LAST;
