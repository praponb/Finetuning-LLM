WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-26 18:00:00'
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on >= DATE '2024-07-09'
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.agent_team,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM combined_details_agent s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    agent_team,
    event_id,
    event_actor
FROM line_level
WHERE agent_team NOT IN ('Tech', 'Billing')
ORDER BY agent_team DESC NULLS FIRST
