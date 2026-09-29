WITH kept_keys AS (
    -- Set intersection on keys
    SELECT dept_name, location
    FROM departments
    INTERSECT ALL
    SELECT dept_name, location
    FROM departments
    WHERE budget <= 1910000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.dept_name = k.dept_name AND s.location = k.location
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        dept_name,
        location,
        RANK() OVER (PARTITION BY location ORDER BY budget) AS budget_rank
    FROM kept_rows
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN location IN ('Tokyo', 'Bangkok') THEN 'Group A' ELSE 'Other' END AS location_group
    FROM enriched
    WHERE dept_name IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(location) AS clean_location,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(location, 'OTHER') AS location_filled
    FROM valid
    WHERE location IS NOT NULL
)
SELECT
    dept_name,
    location,
    location_group,
    dept_name_length,
    location_filled
FROM cleaned
