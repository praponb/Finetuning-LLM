WITH resolved_monthly AS (
    -- resolved rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
replied_monthly AS (
    -- replied rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
SELECT
    COALESCE(a.event_at_month, b.event_at_month) AS event_at_month,
    COALESCE(a.resolved_count, 0) AS resolved_count,
    COALESCE(b.replied_count, 0) AS replied_count,
    COALESCE(a.resolved_count, 0) - COALESCE(b.replied_count, 0) AS resolved_count_minus_replied_count
FROM resolved_monthly a
FULL OUTER JOIN replied_monthly b
  ON a.event_at_month = b.event_at_month
ORDER BY event_at_month
