WITH stacked AS (
    -- Stack the branches
    SELECT operator, qty AS key_id, moved_at
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
    UNION ALL
    SELECT move_type AS operator, sku_id AS key_id, moved_at
    FROM stock_moves
    WHERE operator IS NOT NULL
)
SELECT operator, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(moved_at) AS latest_moved_at
FROM stacked
GROUP BY operator
ORDER BY operator
