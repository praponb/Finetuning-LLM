WITH cleaned AS (
    SELECT
        *,
        COALESCE(location, 'Unknown') AS location_filled,
        COALESCE(dept_name, 'NONE') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location <> 'Berlin'
)
SELECT id
FROM cleaned
EXCEPT DISTINCT
SELECT id
FROM departments
WHERE budget > 3136000
