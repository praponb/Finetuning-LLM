WITH replied_monthly AS (
    -- replied rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
escalated_monthly AS (
    -- escalated rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.event_at_month, b.event_at_month) AS event_at_month,
    COALESCE(a.replied_count, 0) AS replied_count,
    COALESCE(b.escalated_count, 0) AS escalated_count,
    COALESCE(a.replied_count, 0) - COALESCE(b.escalated_count, 0) AS replied_count_minus_escalated_count
FROM replied_monthly a
FULL OUTER JOIN escalated_monthly b
  ON a.event_at_month = b.event_at_month
ORDER BY event_at_month
