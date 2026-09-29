WITH cleaned_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        COALESCE(capacity, 0) AS capacity_filled
    FROM warehouses
    WHERE opened_on >= DATE '2024-11-25'
)
SELECT capacity AS qty, opened_on, id
FROM cleaned_warehouses
UNION
SELECT id, opened_on, capacity
FROM warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
  AND city = 'Kuala Lumpur'
ORDER BY qty, id
