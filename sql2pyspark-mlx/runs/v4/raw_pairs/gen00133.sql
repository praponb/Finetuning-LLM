WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(dept_name) AS clean_dept_name,
        CONCAT_WS('-', location, dept_name) AS location_dept_name_key
    FROM departments
    WHERE location IS NOT NULL
)
SELECT location, id
FROM cleaned
EXCEPT
SELECT location, id
FROM departments
WHERE dept_name = 'HR'
