WITH base_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
      AND city NOT IN ('Kuala Lumpur', 'Singapore')
)
SELECT capacity
FROM base_warehouses
EXCEPT
SELECT capacity
FROM warehouses
ORDER BY capacity
