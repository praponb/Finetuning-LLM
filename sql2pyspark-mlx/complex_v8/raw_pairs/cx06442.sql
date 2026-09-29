WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags
    FROM tickets
),
priority_stats AS (
    -- Avg requester_id per priority
    SELECT priority, AVG(requester_id) AS avg_requester_id
    FROM cleaned
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM cleaned s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id > g.avg_requester_id
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.tags,
        s.avg_requester_id,
        s.requester_id_minus_avg,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM vs_group s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.tags,
        s.avg_requester_id,
        s.agent_hired_on,
        s.agent_name,
        su.tier AS agent_tier
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT *
FROM (
    SELECT agent_hired_on, agent_tier, requester_id
    FROM combined_details_agent
) src
PIVOT (
    SUM(requester_id)
    FOR agent_tier IN ('T3', 'T1')
)
