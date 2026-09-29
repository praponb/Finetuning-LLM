WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE agent_id <= 8
      AND channel NOT IN ('email', 'phone')
)
SELECT id AS entity_id
FROM scoped_tickets
WHERE agent_id >= 4
EXCEPT
SELECT id AS entity_id
FROM ticket_events
ORDER BY entity_id DESC
