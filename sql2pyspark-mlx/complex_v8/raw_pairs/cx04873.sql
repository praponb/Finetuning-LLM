WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tickets
    WHERE csat < 4
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, agent_team, channel, requester_id
    FROM enriched_agent
) src
PIVOT (
    MIN(requester_id)
    FOR channel IN ('web', 'phone', 'chat', 'email')
)
