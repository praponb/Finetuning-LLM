WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price <= 396.4
),
all_events AS (
    -- Stack the branches
    SELECT sku_id, moved_at AS event_time
    FROM valid_moves
    WHERE warehouse_id <= 5
    UNION ALL
    SELECT sku_id, moved_at AS event_time
    FROM stock_moves
    WHERE move_type <> 'ADJUST'
      AND moved_at >= TIMESTAMP '2025-02-12 11:00:00'
)
SELECT sku_id, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY sku_id
ORDER BY sku_id
