WITH outbound_by_warehouse AS (
    -- OUTBOUND rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS outbound_count, SUM(qty) AS outbound_total_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY warehouse_id
),
inbound_by_warehouse AS (
    -- INBOUND rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS inbound_count, SUM(qty) AS inbound_total_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY warehouse_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.warehouse_id, b.warehouse_id) AS warehouse_id,
    COALESCE(a.outbound_count, 0) AS outbound_count,
    COALESCE(a.outbound_total_qty, 0) AS outbound_total_qty,
    COALESCE(b.inbound_count, 0) AS inbound_count,
    b.inbound_total_qty,
    COALESCE(a.outbound_count, 0) - COALESCE(b.inbound_count, 0) AS outbound_count_minus_inbound_count
FROM outbound_by_warehouse a
FULL OUTER JOIN inbound_by_warehouse b
  ON a.warehouse_id = b.warehouse_id
ORDER BY warehouse_id
