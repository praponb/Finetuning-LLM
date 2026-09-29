WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        city,
        capacity,
        opened_on
    FROM warehouses
)
SELECT id
FROM cleaned_warehouses
INTERSECT
SELECT id
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
  AND city IS NOT NULL
ORDER BY id DESC
