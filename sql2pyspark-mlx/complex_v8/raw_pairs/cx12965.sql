WITH recent_tickets AS (
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
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour
    FROM tickets
    WHERE priority IN ('P3', 'P4')
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.closed_at_hour,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        closed_at,
        tags,
        agent_team,
        agent_tier,
        agent_hired_on,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM with_details
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_team, agent_id, agent_tier, requester_id_filled
    FROM scored
) src
PIVOT (
    MAX(requester_id_filled)
    FOR agent_tier IN ('T1', 'T2')
)
ORDER BY agent_team, agent_id
