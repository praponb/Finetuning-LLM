WITH cleaned_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on
    FROM warehouses
    WHERE city = 'Kuala Lumpur'
      AND capacity > 4912
)
SELECT
    id,
    capacity,
    wh_code AS label,
    'warehouse' AS src
FROM cleaned_warehouses
WHERE opened_on BETWEEN DATE '2024-02-19' AND DATE '2025-07-03'
  AND capacity < 11506
UNION
SELECT
    capacity,
    id,
    wh_code,
    'warehouse_2' AS src
FROM warehouses
ORDER BY capacity, id
