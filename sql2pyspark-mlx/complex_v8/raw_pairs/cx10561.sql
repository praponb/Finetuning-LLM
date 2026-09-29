WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        LEAST(csat, requester_id) AS least_csat_requester_id,
        MONTH(opened_at) AS opened_at_month
    FROM tickets
    WHERE channel = 'chat'
      AND agent_id IS NOT NULL
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.opened_at_month,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_id,
    csat,
    agent_tier,
    COUNT(DISTINCT channel) AS distinct_channels
FROM with_details
GROUP BY GROUPING SETS ((agent_id, csat, agent_tier), (agent_id), (csat), ())
ORDER BY agent_id NULLS LAST, csat NULLS LAST, agent_tier NULLS LAST
