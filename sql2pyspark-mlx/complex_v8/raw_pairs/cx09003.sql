WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'agent'
)
SELECT event_type AS label, id
FROM filtered_events
UNION ALL
SELECT agent_name AS label, id
FROM support_agents
WHERE hired_on < DATE '2025-02-26'
