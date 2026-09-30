WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-03-02 08:00:00'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id AND su.team IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team_2
    FROM enriched s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier = 'T3'
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    agent_hired_on,
    agent_team,
    agent_team_2,
    CASE WHEN requester_id >= 20 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier
FROM joined
WHERE agent_team_2 = 'VIP'
ORDER BY agent_id, id DESC
LIMIT 10
