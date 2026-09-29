WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, location
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST, id) = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT location, id
    FROM ranked
    INTERSECT ALL
    SELECT location, id
    FROM departments
    WHERE budget < 3871000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.location = k.location AND s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(location, 1, 2) AS location_prefix
    FROM verified
    WHERE location = 'Singapore'
),
filtered_departments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(location) AS clean_location,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM filtered
    WHERE location != 'Chiang Mai'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary < 101000
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        location,
        location_prefix,
        resolved_location,
        SUBSTRING(location, 1, 4) AS location_prefix_2
    FROM kept
    WHERE location IN ('New York', 'Singapore', 'Tokyo')
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id, location
    FROM cleaned
    EXCEPT
    SELECT id, location
    FROM departments
    WHERE budget > 2201000
),
verified_departments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys_2 k ON s.id = k.id AND s.location = k.location
)
-- Aggregate with conditional metrics
SELECT
    location,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    COUNT(DISTINCT location) AS distinct_locations,
    COUNT(DISTINCT location) AS distinct_locations_2,
    SIZE(COLLECT_SET(location)) AS num_locations,
    SIZE(COLLECT_SET(location)) AS num_locations_2
FROM verified_departments
GROUP BY location
HAVING num_locations_2 > 1
ORDER BY distinct_locations DESC;
