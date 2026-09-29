WITH replied_by_ticket AS (
    -- replied rows per ticket_id
    SELECT ticket_id, COUNT(*) AS replied_count
    FROM ticket_events
    WHERE event_type = 'replied'
    GROUP BY ticket_id
),
reopened_by_ticket AS (
    -- reopened rows per ticket_id
    SELECT ticket_id, COUNT(*) AS reopened_count
    FROM ticket_events
    WHERE event_type = 'reopened'
    GROUP BY ticket_id
)
-- Combine both sides; keys missing on one side get zeros
SELECT
    COALESCE(a.ticket_id, b.ticket_id) AS ticket_id,
    COALESCE(a.replied_count, 0) AS replied_count,
    COALESCE(b.reopened_count, 0) AS reopened_count,
    COALESCE(a.replied_count, 0) - COALESCE(b.reopened_count, 0) AS net_count
FROM replied_by_ticket a
FULL OUTER JOIN reopened_by_ticket b
  ON a.ticket_id = b.ticket_id
ORDER BY ticket_id
