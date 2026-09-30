WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE (move_type IS NOT NULL OR moved_at < TIMESTAMP '2025-02-18 08:00:00')
)
SELECT id AS key_id, move_type AS label
FROM base
EXCEPT ALL
SELECT id, move_type
FROM stock_moves
WHERE moved_at >= TIMESTAMP '2025-01-31 07:00:00'
ORDER BY label, key_id
