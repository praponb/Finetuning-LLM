WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        wh_code,
        capacity,
        opened_on
    FROM warehouses
    WHERE wh_code IS NOT NULL
)
SELECT id
FROM prepared
WHERE capacity < 11005
EXCEPT
SELECT sku_id
FROM stock_moves
WHERE sku_id IS NOT NULL
  AND operator IN ('op_dew', 'op_eve', 'op_chan')
