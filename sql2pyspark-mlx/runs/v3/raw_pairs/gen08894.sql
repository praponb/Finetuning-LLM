SELECT
    location,
    dept_name,
    budget_range,
    unique_locations
FROM (
    SELECT
        location,
        dept_name,
        MAX(budget) - MIN(budget) AS budget_range,
        COUNT(DISTINCT budget) AS unique_locations
    FROM departments
    WHERE budget < 3993000
    GROUP BY location, dept_name
) AS t
WHERE unique_locations > 5;
