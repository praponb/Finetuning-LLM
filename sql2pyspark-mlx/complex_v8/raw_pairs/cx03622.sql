WITH scoped_warehouses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'K%'
      AND city = 'Kuala Lumpur'
)
SELECT capacity AS ref_id
FROM scoped_warehouses
EXCEPT
SELECT capacity AS ref_id
FROM warehouses
ORDER BY ref_id
