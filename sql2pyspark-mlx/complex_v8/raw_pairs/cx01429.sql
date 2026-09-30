WITH unioned AS (
    -- Stack the branches
    SELECT id AS ref_id, moved_at AS happened_at, move_type AS label
    FROM stock_moves
    WHERE sku_id <= 13
      AND moved_at >= TIMESTAMP '2025-01-13 09:00:00'
    UNION ALL
    SELECT warehouse_id, moved_at, move_type
    FROM stock_moves
    WHERE operator = 'op_ann'
      AND sku_id <= 14
    UNION ALL
    SELECT id, moved_at, operator
    FROM stock_moves
    WHERE move_type = 'INBOUND'
)
SELECT label, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(happened_at) AS latest_happened_at
FROM unioned
GROUP BY label
ORDER BY label
