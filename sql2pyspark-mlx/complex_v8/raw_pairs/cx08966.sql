WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        ABS(csat - 3) AS csat_distance
    FROM tickets
    WHERE requester_id <= 16
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier, su.agent_name
    FROM recent s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on >= DATE '2024-07-09'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.tags,
        s.csat_distance,
        su.tier AS agent_tier_2,
        su.hired_on AS agent_hired_on
    FROM enriched_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    priority,
    csat_distance,
    agent_tier_2,
    agent_hired_on
FROM with_details
ORDER BY agent_hired_on DESC, agent_tier_2
