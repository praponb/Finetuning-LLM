WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(location) AS clean_location,
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
        budget
    FROM base
) src
PIVOT (
    AVG(budget) AS total,
    COUNT(1) AS n
    FOR location IN ('Singapore', 'London')
);
