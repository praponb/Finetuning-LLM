WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE opened_on < DATE '2025-02-14'
      AND city IN ('Bangkok', 'Kuala Lumpur')
)
SELECT
    'warehouse' AS record_type,
    capacity,
    wh_code,
    capacity_filled AS key_id
FROM cleaned_warehouses
UNION ALL
SELECT
    'warehouse_2' AS record_type,
    id AS capacity,
    wh_code,
    capacity AS key_id
FROM warehouses
WHERE opened_on < DATE '2025-07-03'
