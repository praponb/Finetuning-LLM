WITH outbound_monthly AS (
    -- OUTBOUND rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS outbound_count, SUM(qty) AS outbound_total_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
),
transfer_monthly AS (
    -- TRANSFER rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
)
SELECT
    COALESCE(a.moved_at_month, b.moved_at_month) AS moved_at_month,
    COALESCE(a.outbound_count, 0) AS outbound_count,
    a.outbound_total_qty,
    COALESCE(b.transfer_count, 0) AS transfer_count,
    b.transfer_total_qty,
    COALESCE(a.outbound_count, 0) - COALESCE(b.transfer_count, 0) AS outbound_count_minus_transfer_count
FROM outbound_monthly a
FULL OUTER JOIN transfer_monthly b
  ON a.moved_at_month = b.moved_at_month
ORDER BY moved_at_month
