WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
      AND priority <> 'P1'
)
SELECT agent_id AS qty
FROM scoped_tickets
EXCEPT ALL
SELECT id AS qty
FROM support_agents
