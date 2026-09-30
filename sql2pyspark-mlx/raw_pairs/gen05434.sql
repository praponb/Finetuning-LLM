WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(budget, 2) AS budget_rounded,
        ROUND(budget, 1) AS budget_rounded_2,
        COALESCE(location, 'N/A') AS resolved_location
    FROM departments
    WHERE location IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_name,
        location,
        budget_rounded_2
    FROM base
) src
PIVOT (
    SUM(budget_rounded_2)
    FOR location IN ('Tokyo' AS tokyo, 'Bangkok' AS bangkok, 'New York' AS new_york)
);
