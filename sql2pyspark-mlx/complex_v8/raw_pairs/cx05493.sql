WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (event_at < TIMESTAMP '2025-02-18 11:00:00' OR actor = 'system')
)
SELECT ticket_id AS qty
FROM scoped_events
WHERE actor = 'customer'
  AND event_type <> 'replied'
EXCEPT
SELECT requester_id AS qty
FROM tickets
ORDER BY qty
