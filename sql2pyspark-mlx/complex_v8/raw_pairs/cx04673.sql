WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 07:00:00'
)
SELECT id
FROM cleaned
INTERSECT
SELECT id
FROM skus
ORDER BY id DESC
