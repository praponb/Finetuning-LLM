WITH filtered AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city NOT IN ('Kuala Lumpur', 'Bangkok')
      AND LENGTH(TRIM(wh_code)) >= 4
)
SELECT city, opened_on, 'warehouse' AS record_type
FROM filtered
UNION
SELECT city, opened_on, 'warehouse_2' AS record_type
FROM warehouses
WHERE LOWER(wh_code) LIKE '%nx%'
  AND capacity >= 4912
ORDER BY opened_on, record_type
