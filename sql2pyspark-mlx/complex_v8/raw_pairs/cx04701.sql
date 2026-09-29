WITH reopened_by_ticket AS (
    -- reopened rows per ticket_id
    SELECT ticket_id, COUNT(*) AS reopened_count
    FROM ticket_events
    WHERE event_type = 'reopened'
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
    COALESCE(a.reopened_count, 0) AS reopened_count,
    COALESCE(b.resolved_count, 0) AS resolved_count,
    COALESCE(a.reopened_count, 0) - COALESCE(b.resolved_count, 0) AS net_count
FROM reopened_by_ticket a
FULL OUTER JOIN resolved_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
