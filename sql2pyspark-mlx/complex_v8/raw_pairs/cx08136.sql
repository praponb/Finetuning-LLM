WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        QUARTER(moved_at) AS moved_at_quarter
    FROM stock_moves
    WHERE sku_id <= 13
      AND moved_at >= TIMESTAMP '2025-01-31 08:00:00'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        moved_at_quarter,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
    FROM cleaned
    WHERE qty <= 387
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, qty, moved_at_quarter
    FROM scored
) src
UNPIVOT (
    reading FOR field IN (qty, moved_at_quarter)
) unp
