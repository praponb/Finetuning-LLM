WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel
    FROM tickets
    WHERE channel NOT IN ('email', 'web')
      AND requester_id < 22
),
combined AS (
    -- Stack the branches
    SELECT requester_id, agent_id AS entity_id
    FROM prepared
    WHERE channel <> 'email'
    UNION ALL
    SELECT ticket_id AS requester_id, id AS entity_id
    FROM ticket_events
    WHERE actor = 'agent'
    UNION ALL
    SELECT id AS requester_id, ticket_id AS entity_id
    FROM ticket_events
)
SELECT requester_id, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY requester_id
ORDER BY requester_id
