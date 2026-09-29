WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        city,
        opened_on,
        LENGTH(TRIM(wh_code)) AS wh_code_len
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
)
SELECT wh_code_len AS key_id, id AS qty
FROM prepared
WHERE YEAR(opened_on) = 2025
  AND wh_code_len >= 6
UNION
SELECT id, warehouse_id
FROM stock_moves
WHERE move_type IS NOT NULL
  AND operator IS NOT NULL
ORDER BY qty
