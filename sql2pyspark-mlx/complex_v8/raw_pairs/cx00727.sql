WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
      AND event_type NOT IN ('assigned', 'replied')
)
SELECT actor, event_type, event_at AS happened_at
FROM scoped_events
WHERE event_at >= TIMESTAMP '2025-01-25 09:00:00'
  AND actor = 'agent'
UNION
SELECT event_type, actor, event_at
FROM ticket_events
WHERE event_type = 'created'
