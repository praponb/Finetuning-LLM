WITH inbound_by_warehouse AS (
    -- INBOUND rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS inbound_count, SUM(qty) AS inbound_total_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY warehouse_id
),
transfer_by_warehouse AS (
    -- TRANSFER rows per warehouse_id
    SELECT warehouse_id, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY warehouse_id
)
SELECT
    COALESCE(a.warehouse_id, b.warehouse_id) AS warehouse_id,
    COALESCE(a.inbound_count, 0) AS inbound_count,
    COALESCE(a.inbound_total_qty, 0) AS inbound_total_qty,
    COALESCE(b.transfer_count, 0) AS transfer_count,
    b.transfer_total_qty,
    COALESCE(a.inbound_count, 0) - COALESCE(b.transfer_count, 0) AS inbound_count_minus_transfer_count
FROM inbound_by_warehouse a
FULL OUTER JOIN transfer_by_warehouse b
  ON a.warehouse_id = b.warehouse_id
ORDER BY warehouse_id
