WITH resolved_by_ticket AS (
    -- resolved rows per ticket_id
    SELECT ticket_id, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
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
    COALESCE(a.resolved_count, 0) AS resolved_count,
    COALESCE(b.escalated_count, 0) AS escalated_count,
    COALESCE(a.resolved_count, 0) - COALESCE(b.escalated_count, 0) AS net_count
FROM resolved_by_ticket a
FULL OUTER JOIN escalated_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
