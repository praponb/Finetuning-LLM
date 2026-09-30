WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        ARRAY_CONTAINS(tags, 'login') AS has_login
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 09:00:00'
)
SELECT id
FROM scoped_tickets
INTERSECT
SELECT id
FROM ticket_events
WHERE actor <> 'system'
  AND ticket_id IS NOT NULL
ORDER BY id DESC
