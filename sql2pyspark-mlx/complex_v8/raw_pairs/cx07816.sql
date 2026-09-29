WITH escalated_monthly AS (
    -- escalated rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
reopened_monthly AS (
    -- reopened rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS reopened_count
    FROM ticket_events
    WHERE event_type = 'reopened'
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.event_at_month, b.event_at_month) AS event_at_month,
    COALESCE(a.escalated_count, 0) AS escalated_count,
    COALESCE(b.reopened_count, 0) AS reopened_count,
    COALESCE(a.escalated_count, 0) - COALESCE(b.reopened_count, 0) AS net_count
FROM escalated_monthly a
FULL OUTER JOIN reopened_monthly b
  ON a.event_at_month = b.event_at_month
ORDER BY event_at_month
