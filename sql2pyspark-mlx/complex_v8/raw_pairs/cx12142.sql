WITH inbound_monthly AS (
    -- INBOUND rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS inbound_count, ROUND(AVG(qty), 2) AS inbound_avg_qty
    FROM stock_moves
    WHERE move_type = 'INBOUND'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
),
outbound_monthly AS (
    -- OUTBOUND rows per moved_at_month
    SELECT DATE_TRUNC('MONTH', moved_at) AS moved_at_month, COUNT(*) AS outbound_count, ROUND(AVG(qty), 2) AS outbound_avg_qty
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
    GROUP BY DATE_TRUNC('MONTH', moved_at)
)
SELECT
    COALESCE(a.moved_at_month, b.moved_at_month) AS moved_at_month,
    COALESCE(a.inbound_count, 0) AS inbound_count,
    COALESCE(a.inbound_avg_qty, 0) AS inbound_avg_qty,
    COALESCE(b.outbound_count, 0) AS outbound_count,
    COALESCE(b.outbound_avg_qty, 0) AS outbound_avg_qty,
    COALESCE(a.inbound_count, 0) - COALESCE(b.outbound_count, 0) AS net_count
FROM inbound_monthly a
FULL OUTER JOIN outbound_monthly b
  ON a.moved_at_month = b.moved_at_month
ORDER BY moved_at_month
