WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 9 AND 11
      AND priority IS NOT NULL
)
SELECT id, agent_id
FROM prepared_tickets
WHERE channel = 'email'
UNION ALL
SELECT requester_id, id
FROM tickets
UNION ALL
SELECT requester_id, id
FROM tickets
