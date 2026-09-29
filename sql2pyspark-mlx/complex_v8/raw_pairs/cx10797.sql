WITH cleaned_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        UPPER(TRIM(wh_code)) AS wh_code_clean
    FROM warehouses
    WHERE (capacity < 11768 OR wh_code IS NOT NULL)
)
SELECT capacity, wh_code AS name
FROM cleaned_warehouses
WHERE LENGTH(TRIM(wh_code)) >= 8
UNION ALL
SELECT warehouse_id, move_type
FROM stock_moves
UNION ALL
SELECT id, wh_code
FROM warehouses
WHERE capacity <= 11005
  AND city <> 'Singapore'
