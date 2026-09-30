WITH transfer_monthly AS (
    -- TRANSFER rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS transfer_count, ROUND(AVG(qty), 2) AS transfer_avg_qty
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
),
inbound_monthly AS (
    -- INBOUND rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS inbound_count, SUM(qty) AS inbound_total_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
)
SELECT
    COALESCE(a.moved_at_month, b.moved_at_month) AS moved_at_month,
    COALESCE(a.transfer_count, 0) AS transfer_count,
    a.transfer_avg_qty,
    COALESCE(b.inbound_count, 0) AS inbound_count,
    b.inbound_total_qty,
    COALESCE(a.transfer_count, 0) - COALESCE(b.inbound_count, 0) AS transfer_count_minus_inbound_count
FROM transfer_monthly a
FULL OUTER JOIN inbound_monthly b
  ON a.moved_at_month = b.moved_at_month
ORDER BY moved_at_month
