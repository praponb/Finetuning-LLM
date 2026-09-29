WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('created', 'reopened')
)
SELECT id AS key_id, actor
FROM filtered
WHERE event_type NOT IN ('assigned', 'created')
  AND MONTH(event_at) IN (1, 2)
INTERSECT
SELECT id, channel
FROM tickets
WHERE channel NOT IN ('web', 'chat')
  AND csat >= 2
