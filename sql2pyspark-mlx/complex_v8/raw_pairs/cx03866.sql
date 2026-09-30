WITH all_events AS (
    -- Stack the branches
    SELECT warehouse_id AS key_id, moved_at AS happened_at
    FROM stock_moves
    UNION ALL
    SELECT qty, moved_at
    FROM stock_moves
    WHERE move_type = 'ADJUST'
)
SELECT key_id, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM all_events
GROUP BY key_id
ORDER BY key_id
