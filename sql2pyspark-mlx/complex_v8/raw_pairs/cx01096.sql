WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE move_type = 'INBOUND'
)
SELECT
    sku_id,
    moved_at AS event_time,
    unit_price AS value,
    'move' AS record_type
FROM stock_moves
WHERE warehouse_id <= 5
  AND unit_price >= 80.5
UNION ALL
SELECT
    id,
    moved_at,
    unit_price,
    'move_2' AS record_type
FROM stock_moves
