WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM stock_moves
    WHERE (unit_price >= 60.4 OR warehouse_id IS NOT NULL)
)
SELECT id AS ref_id, moved_at, warehouse_id
FROM stock_moves
UNION ALL
SELECT warehouse_id AS ref_id, moved_at, sku_id AS warehouse_id
FROM stock_moves
WHERE warehouse_id <= 5
ORDER BY moved_at
