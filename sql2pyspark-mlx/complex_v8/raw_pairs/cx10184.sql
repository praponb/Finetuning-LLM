WITH resolved_by_ticket AS (
    -- resolved rows per ticket_id
    SELECT ticket_id, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
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
    COALESCE(a.resolved_count, 0) AS resolved_count,
    COALESCE(b.replied_count, 0) AS replied_count,
    COALESCE(a.resolved_count, 0) - COALESCE(b.replied_count, 0) AS resolved_count_minus_replied_count
FROM resolved_by_ticket a
FULL OUTER JOIN replied_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
