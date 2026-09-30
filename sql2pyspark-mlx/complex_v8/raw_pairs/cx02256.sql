WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 10:00:00'
      AND qty >= 112
)
SELECT
    qty,
    move_type,
    moved_at,
    'move' AS source
FROM stock_moves
UNION ALL
SELECT
    id,
    operator,
    moved_at,
    'move_2' AS source
FROM stock_moves
ORDER BY source
