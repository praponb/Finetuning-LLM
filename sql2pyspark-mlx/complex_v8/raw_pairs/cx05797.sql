WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags
    FROM tickets
    WHERE channel IS NOT NULL
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.tags,
        su.team AS agent_team
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        channel,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM with_details_agent
    WHERE channel IN ('chat', 'phone')
),
combined_details AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier, su.team AS agent_team
    FROM derived s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_tier,
    agent_id,
    ROUND(100 * COUNT(CASE WHEN channel IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_channel_is_not_null,
    COUNT(DISTINCT channel) AS distinct_channels
FROM combined_details
GROUP BY ROLLUP(agent_tier, agent_id)
ORDER BY agent_tier NULLS LAST, agent_id NULLS LAST
