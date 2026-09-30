WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE operator IN ('op_chan', 'op_bee', 'op_dew')
)
SELECT sku_id AS key_id, move_type AS name
FROM recent
WHERE unit_price > 215.4
UNION
SELECT id AS key_id, operator AS name
FROM stock_moves
WHERE sku_id IS NOT NULL
  AND unit_price > 238.5
ORDER BY name DESC, key_id
