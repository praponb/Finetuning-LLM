WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND ARRAY_CONTAINS(t.tags, 'login')
              AND t.csat > 3
        )
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id
FROM qualified
WHERE agent_id >= 2
ORDER BY event_type, id
LIMIT 10
