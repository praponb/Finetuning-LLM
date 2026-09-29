WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at
    FROM stock_moves
    WHERE operator <> 'op_ann'
),
unioned AS (
    -- Stack the branches
    SELECT id, move_type AS name, 'move' AS record_type
    FROM filtered
    WHERE sku_id <= 19
      AND move_type IS NOT NULL
    UNION ALL
    SELECT id, wh_code AS name, 'warehouse' AS record_type
    FROM warehouses
    WHERE wh_code IS NOT NULL
      AND city = 'Kuala Lumpur'
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY record_type
ORDER BY record_type
