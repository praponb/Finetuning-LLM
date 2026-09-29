WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(dept_name) dept_name_upper,
        COALESCE(location, 'N/A') resolved_location
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'New York'
)
SELECT id, dept_name
FROM filtered
INTERSECT ALL
SELECT id, dept_name
FROM departments
WHERE location != 'New York'
