WITH transfer_by_warehouse AS (
    -- TRANSFER rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY warehouse_id
),
inbound_by_warehouse AS (
    -- INBOUND rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS inbound_count, ROUND(AVG(qty), 2) AS inbound_avg_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY warehouse_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.warehouse_id, b.warehouse_id) AS warehouse_id,
    COALESCE(a.transfer_count, 0) AS transfer_count,
    a.transfer_total_qty,
    COALESCE(b.inbound_count, 0) AS inbound_count,
    b.inbound_avg_qty,
    COALESCE(a.transfer_count, 0) - COALESCE(b.inbound_count, 0) AS transfer_count_minus_inbound_count
FROM transfer_by_warehouse a
FULL OUTER JOIN inbound_by_warehouse b
  ON a.warehouse_id = b.warehouse_id
ORDER BY warehouse_id
