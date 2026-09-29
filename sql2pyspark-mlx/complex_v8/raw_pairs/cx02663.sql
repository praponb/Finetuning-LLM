WITH combined AS (
    -- Stack the branches
    SELECT sku_id, moved_at
    FROM stock_moves
    UNION
    SELECT sku_id, moved_at
    FROM stock_moves
)
SELECT sku_id, COUNT(*) AS n_rows, MAX(moved_at) AS latest_moved_at
FROM combined
GROUP BY sku_id
ORDER BY sku_id
