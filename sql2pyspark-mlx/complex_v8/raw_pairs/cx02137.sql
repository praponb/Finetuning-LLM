WITH outbound_by_warehouse AS (
    -- OUTBOUND rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS outbound_count, SUM(qty) AS outbound_total_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY warehouse_id
),
transfer_by_warehouse AS (
    -- TRANSFER rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY warehouse_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.warehouse_id, b.warehouse_id) AS warehouse_id,
    COALESCE(a.outbound_count, 0) AS outbound_count,
    a.outbound_total_qty,
    COALESCE(b.transfer_count, 0) AS transfer_count,
    b.transfer_total_qty,
    COALESCE(a.outbound_count, 0) - COALESCE(b.transfer_count, 0) AS outbound_count_minus_transfer_count
FROM outbound_by_warehouse a
FULL OUTER JOIN transfer_by_warehouse b
  ON a.warehouse_id = b.warehouse_id
ORDER BY warehouse_id
