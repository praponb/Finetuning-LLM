WITH resolved_by_ticket AS (
    -- resolved rows per ticket_id
    SELECT ticket_id, COUNT(*) AS resolved_count
    FROM ticket_events
    WHERE event_type = 'resolved'
    GROUP BY ticket_id
),
reopened_by_ticket AS (
    -- reopened rows per ticket_id
    SELECT ticket_id, COUNT(*) AS reopened_count
    FROM ticket_events
    WHERE event_type = 'reopened'
    GROUP BY ticket_id
)
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.resolved_count, 0) AS resolved_count,
    COALESCE(b.reopened_count, 0) AS reopened_count,
    COALESCE(a.resolved_count, 0) - COALESCE(b.reopened_count, 0) AS resolved_count_minus_reopened_count
FROM resolved_by_ticket a
FULL OUTER JOIN reopened_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
