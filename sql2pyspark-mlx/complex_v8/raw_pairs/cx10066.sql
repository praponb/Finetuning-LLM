WITH escalated_by_ticket AS (
    -- escalated rows per ticket_id
    SELECT ticket_id, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY ticket_id
),
replied_by_ticket AS (
    -- replied rows per ticket_id
    SELECT ticket_id, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY ticket_id
)
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.escalated_count, 0) AS escalated_count,
    COALESCE(b.replied_count, 0) AS replied_count,
    COALESCE(a.escalated_count, 0) - COALESCE(b.replied_count, 0) AS net_count
FROM escalated_by_ticket a
FULL OUTER JOIN replied_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
