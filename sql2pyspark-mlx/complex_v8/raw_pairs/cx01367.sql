WITH escalated_by_ticket AS (
    -- escalated rows per ticket_id
    SELECT ticket_id, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY ticket_id
),
resolved_by_ticket AS (
    -- resolved rows per ticket_id
    SELECT ticket_id, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
    GROUP BY ticket_id
)
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.escalated_count, 0) AS escalated_count,
    COALESCE(b.resolved_count, 0) AS resolved_count,
    COALESCE(a.escalated_count, 0) - COALESCE(b.resolved_count, 0) AS net_count
FROM escalated_by_ticket a
FULL OUTER JOIN resolved_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
