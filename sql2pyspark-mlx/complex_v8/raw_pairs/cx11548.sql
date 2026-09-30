WITH reopened_monthly AS (
    -- reopened rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS reopened_count
    FROM ticket_events
    WHERE event_type = 'reopened'
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
replied_monthly AS (
    -- replied rows per event_at_month
    SELECT DATE_TRUNC('MONTH', event_at) AS event_at_month, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.event_at_month, b.event_at_month) AS event_at_month,
    COALESCE(a.reopened_count, 0) AS reopened_count,
    COALESCE(b.replied_count, 0) AS replied_count,
    COALESCE(a.reopened_count, 0) - COALESCE(b.replied_count, 0) AS reopened_count_minus_replied_count
FROM reopened_monthly a
FULL OUTER JOIN replied_monthly b
  ON a.event_at_month = b.event_at_month
ORDER BY event_at_month
