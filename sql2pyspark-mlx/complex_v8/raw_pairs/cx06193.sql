WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj
    FROM stock_moves
    WHERE move_type IS NOT NULL
),
all_events AS (
    -- Stack the branches
    SELECT sku_id, moved_at AS happened_at, id AS qty
    FROM base_moves
    UNION ALL
    SELECT warehouse_id, moved_at, qty
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
      AND operator = 'op_bee'
)
SELECT sku_id, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(happened_at) AS latest_happened_at
FROM all_events
GROUP BY sku_id
ORDER BY sku_id
