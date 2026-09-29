WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE move_type NOT IN ('INBOUND', 'ADJUST')
),
combined AS (
    -- Stack the branches
    SELECT operator AS label, moved_at, 'move' AS record_type
    FROM cleaned
    UNION
    SELECT operator, moved_at, 'move_2' AS record_type
    FROM stock_moves
    WHERE sku_id IS NOT NULL
)
SELECT record_type, COUNT(*) AS n_rows, MAX(moved_at) AS latest_moved_at
FROM combined
GROUP BY record_type
ORDER BY record_type
