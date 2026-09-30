WITH replied_monthly AS (
    -- replied rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
resolved_monthly AS (
    -- resolved rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.event_at_month, b.event_at_month) AS event_at_month,
    COALESCE(a.replied_count, 0) AS replied_count,
    COALESCE(b.resolved_count, 0) AS resolved_count,
    COALESCE(a.replied_count, 0) - COALESCE(b.resolved_count, 0) AS net_count
FROM replied_monthly a
FULL OUTER JOIN resolved_monthly b
  ON a.event_at_month = b.event_at_month
ORDER BY event_at_month
