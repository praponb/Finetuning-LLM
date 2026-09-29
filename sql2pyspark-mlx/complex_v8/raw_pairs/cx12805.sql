WITH scoped_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        LENGTH(TRIM(wh_code)) AS wh_code_len
    FROM warehouses
    WHERE capacity <= 11506
      AND LENGTH(TRIM(wh_code)) >= 7
)
SELECT wh_code_len AS key_id
FROM scoped_warehouses
WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
INTERSECT
SELECT id AS key_id
FROM warehouses
WHERE capacity <= 11506
