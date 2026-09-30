WITH escalated_monthly AS (
    -- escalated rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
resolved_monthly AS (
    -- resolved rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
SELECT
    COALESCE(a.event_at_month, b.event_at_month) AS event_at_month,
    COALESCE(a.escalated_count, 0) AS escalated_count,
    COALESCE(b.resolved_count, 0) AS resolved_count,
    COALESCE(a.escalated_count, 0) - COALESCE(b.resolved_count, 0) AS net_count
FROM escalated_monthly a
FULL OUTER JOIN resolved_monthly b
  ON a.event_at_month = b.event_at_month
ORDER BY event_at_month
