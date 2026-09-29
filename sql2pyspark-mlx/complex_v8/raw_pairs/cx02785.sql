WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type <> 'OUTBOUND'
)
SELECT id AS ref_id, moved_at
FROM prepared_moves
WHERE sku_id IS NOT NULL
  AND move_type = 'TRANSFER'
UNION
SELECT id AS ref_id, moved_at
FROM stock_moves
ORDER BY ref_id DESC
