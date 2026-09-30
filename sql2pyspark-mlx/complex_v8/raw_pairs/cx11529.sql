WITH transfer_monthly AS (
    -- TRANSFER rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS transfer_count, SUM(qty) AS transfer_total_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
),
outbound_monthly AS (
    -- OUTBOUND rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS outbound_count, ROUND(AVG(qty), 2) AS outbound_avg_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.moved_at_month, b.moved_at_month) AS moved_at_month,
    COALESCE(a.transfer_count, 0) AS transfer_count,
    a.transfer_total_qty,
    COALESCE(b.outbound_count, 0) AS outbound_count,
    COALESCE(b.outbound_avg_qty, 0) AS outbound_avg_qty,
    COALESCE(a.transfer_count, 0) - COALESCE(b.outbound_count, 0) AS net_count
FROM transfer_monthly a
FULL OUTER JOIN outbound_monthly b
  ON a.moved_at_month = b.moved_at_month
ORDER BY moved_at_month
