WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE operator <> 'op_dew'
      AND unit_price <= 372.6
)
SELECT
    sku_id,
    qty,
    moved_at,
    'move' AS origin
FROM cleaned_moves
WHERE moved_at >= TIMESTAMP '2025-02-06 10:00:00'
UNION
SELECT
    qty,
    sku_id,
    moved_at,
    'move_2' AS origin
FROM stock_moves
ORDER BY qty DESC, origin
