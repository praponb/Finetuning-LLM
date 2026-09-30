WITH all_events AS (
    -- Stack the branches
    SELECT qty AS key_id, moved_at, move_type
    FROM stock_moves
    WHERE sku_id >= 18
      AND qty BETWEEN 89 AND 423
    UNION ALL
    SELECT sku_id, moved_at, move_type
    FROM stock_moves
    UNION ALL
    SELECT id, moved_at, operator
    FROM stock_moves
)
SELECT move_type, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(moved_at) AS latest_moved_at
FROM all_events
GROUP BY move_type
ORDER BY move_type
