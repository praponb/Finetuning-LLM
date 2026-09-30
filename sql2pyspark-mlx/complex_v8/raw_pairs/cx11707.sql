WITH reopened_by_ticket AS (
    -- reopened rows per ticket_id
    SELECT ticket_id, COUNT(*) AS reopened_count
    FROM ticket_events
    WHERE event_type = 'reopened'
    GROUP BY ticket_id
),
escalated_by_ticket AS (
    -- escalated rows per ticket_id
    SELECT ticket_id, COUNT(*) AS escalated_count
    FROM ticket_events
    WHERE event_type = 'escalated'
    GROUP BY ticket_id
)
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.reopened_count, 0) AS reopened_count,
    COALESCE(b.escalated_count, 0) AS escalated_count,
    COALESCE(a.reopened_count, 0) - COALESCE(b.escalated_count, 0) AS reopened_count_minus_escalated_count
FROM reopened_by_ticket a
FULL OUTER JOIN escalated_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
