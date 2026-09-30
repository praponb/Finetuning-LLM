WITH replied_by_ticket AS (
    -- replied rows per ticket_id
    SELECT ticket_id, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY ticket_id
),
escalated_by_ticket AS (
    -- escalated rows per ticket_id
    SELECT ticket_id, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY ticket_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.replied_count, 0) AS replied_count,
    COALESCE(b.escalated_count, 0) AS escalated_count,
    COALESCE(a.replied_count, 0) - COALESCE(b.escalated_count, 0) AS replied_count_minus_escalated_count
FROM replied_by_ticket a
FULL OUTER JOIN escalated_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
