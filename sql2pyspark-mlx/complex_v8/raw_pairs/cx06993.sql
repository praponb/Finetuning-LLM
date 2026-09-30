WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ABS(qty - 138) AS qty_distance,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-08 09:00:00'
      AND move_type <> 'TRANSFER'
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM recent_moves
    WHERE qty_distance > (SELECT MIN(qty_distance) FROM recent_moves)
)
SELECT
    move_type,
    operator,
    sku_id,
    GROUPING(move_type) AS is_total_move_type,
    ROUND(STDDEV(qty), 2) AS stddev_qty,
    MIN(moved_at) AS first_moved_at
FROM without_matches
GROUP BY CUBE(move_type, operator, sku_id)
