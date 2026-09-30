WITH replied_by_ticket AS (
    -- replied rows per ticket_id
    SELECT ticket_id, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY ticket_id
),
resolved_by_ticket AS (
    -- resolved rows per ticket_id
    SELECT ticket_id, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
    GROUP BY ticket_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.replied_count, 0) AS replied_count,
    COALESCE(b.resolved_count, 0) AS resolved_count,
    COALESCE(a.replied_count, 0) - COALESCE(b.resolved_count, 0) AS replied_count_minus_resolved_count
FROM replied_by_ticket a
FULL OUTER JOIN resolved_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
