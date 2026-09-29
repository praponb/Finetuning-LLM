WITH recent AS (
    -- Scope the warehouses rows
    SELECT *
    FROM warehouses
)
SELECT capacity, opened_on, 'warehouse' AS source
FROM recent
WHERE LOWER(wh_code) LIKE '%x1%'
  AND YEAR(opened_on) = 2025
UNION ALL
SELECT capacity, opened_on, 'warehouse_2' AS source
FROM warehouses
WHERE capacity <= 11506
  AND city = 'Bangkok'
ORDER BY source
