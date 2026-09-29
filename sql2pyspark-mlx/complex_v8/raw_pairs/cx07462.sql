WITH combined AS (
    -- Stack the branches
    SELECT id AS key_id, qty, moved_at AS happened_at
    FROM stock_moves
    WHERE move_type = 'ADJUST'
    UNION ALL
    SELECT warehouse_id, sku_id, moved_at
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
      AND qty > 169
)
SELECT key_id, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY key_id
ORDER BY key_id
