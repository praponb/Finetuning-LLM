WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        CONCAT_WS('|', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE priority = 'P4'
      AND closed_at < TIMESTAMP '2025-02-26 16:00:00'
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE csat < (SELECT MAX(csat) FROM filtered)
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_priority_label,
        su.tier AS agent_tier
    FROM without_matches s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT *
FROM (
    SELECT agent_id, priority, csat
    FROM enriched_agent
) src
PIVOT (
    MAX(csat)
    FOR priority IN ('P2', 'P4')
)
ORDER BY agent_id
