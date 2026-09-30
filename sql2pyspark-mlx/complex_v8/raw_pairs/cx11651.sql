WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE agent_id >= 5
      AND HOUR(opened_at) BETWEEN 7 AND 13
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    csat,
    COUNT(*) AS n_rows,
    SUM(requester_id) FILTER (WHERE agent_tier = 'T2') AS requester_id_agent_tier_t2,
    MAX(closed_at) AS last_closed_at
FROM with_parent_agent
GROUP BY GROUPING SETS ((agent_id, csat), (agent_id), ())
ORDER BY agent_id NULLS LAST, csat NULLS LAST
