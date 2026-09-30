WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN dept_name IN ('Sales', 'HR') THEN 'Priority' ELSE 'Other' END AS dept_name_group,
        COALESCE(location, 'unassigned') AS location_filled,
        COALESCE(location, 'NONE') AS location_clean
    FROM departments
    WHERE location IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT location
    FROM filtered
    INTERSECT
    SELECT location
    FROM departments
    WHERE dept_name IN ('HR', 'Sales')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.location = k.location
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM reconciled
    EXCEPT
    SELECT id
    FROM departments
    WHERE dept_name IN ('HR', 'Legal', 'Engineering')
),
reconciled_departments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget, 1) AS budget_rounded,
        CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END AS is_hr,
        COALESCE(location, 'UNKNOWN') AS location_clean_2
    FROM reconciled_departments
    WHERE location IN ('Chiang Mai', 'Singapore')
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            dept_name,
            location,
            budget_rounded
        FROM valid
    ) src
    PIVOT (
        SUM(budget_rounded)
        FOR location IN ('London', 'Tokyo', 'Bangkok')
    )
)
SELECT
    dept_name,
    Tokyo,
    Bangkok
FROM pivoted
ORDER BY Tokyo NULLS FIRST, Bangkok;
