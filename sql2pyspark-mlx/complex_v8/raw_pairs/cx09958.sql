WITH all_events AS (
    -- Stack the branches
    SELECT move_type, moved_at
    FROM stock_moves
    UNION ALL
    SELECT operator AS move_type, moved_at
    FROM stock_moves
)
SELECT move_type, COUNT(*) AS n_rows, MAX(moved_at) AS latest_moved_at
FROM all_events
GROUP BY move_type
ORDER BY move_type
