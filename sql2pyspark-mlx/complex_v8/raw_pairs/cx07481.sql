WITH transfer_by_warehouse AS (
    -- TRANSFER rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS transfer_count, ROUND(AVG(qty), 2) AS transfer_avg_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY warehouse_id
),
outbound_by_warehouse AS (
    -- OUTBOUND rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS outbound_count, ROUND(AVG(qty), 2) AS outbound_avg_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY warehouse_id
)
SELECT
    COALESCE(a.warehouse_id, b.warehouse_id) AS warehouse_id,
    COALESCE(a.transfer_count, 0) AS transfer_count,
    COALESCE(a.transfer_avg_qty, 0) AS transfer_avg_qty,
    COALESCE(b.outbound_count, 0) AS outbound_count,
    COALESCE(b.outbound_avg_qty, 0) AS outbound_avg_qty,
    COALESCE(a.transfer_count, 0) - COALESCE(b.outbound_count, 0) AS transfer_count_minus_outbound_count
FROM transfer_by_warehouse a
FULL OUTER JOIN outbound_by_warehouse b
  ON a.warehouse_id = b.warehouse_id
ORDER BY warehouse_id
