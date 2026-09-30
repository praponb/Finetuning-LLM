WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        tags
    FROM tickets
    WHERE requester_id < 18
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM prepared_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    tags,
    agent_hired_on,
    agent_team,
    DATE_TRUNC('WEEK', agent_hired_on) AS agent_hired_on_week
FROM enriched_agent
ORDER BY agent_hired_on_week DESC
