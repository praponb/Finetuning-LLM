WITH valid_moves AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND TO_DATE(moved_at) < DATE '2025-03-02'
)
SELECT warehouse_id AS qty, moved_at
FROM valid_moves
UNION ALL
SELECT warehouse_id AS qty, moved_at
FROM stock_moves
ORDER BY qty
