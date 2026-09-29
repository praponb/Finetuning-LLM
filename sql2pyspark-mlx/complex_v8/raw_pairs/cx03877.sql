WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 09:00:00'
)
SELECT id
FROM recent_events
WHERE actor IN ('agent', 'system')
  AND event_type NOT IN ('reopened', 'resolved')
INTERSECT
SELECT id
FROM tickets
WHERE csat > 2
  AND closed_at >= TIMESTAMP '2025-01-13 14:00:00'
