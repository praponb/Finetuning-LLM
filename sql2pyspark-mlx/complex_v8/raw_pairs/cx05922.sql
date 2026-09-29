WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        CASE WHEN channel IN ('phone', 'chat') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.agent_name
    FROM filtered_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    channel,
    tags,
    agent_name,
    INITCAP(TRIM(channel_group)) AS channel_group_clean,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
FROM combined_details_agent
ORDER BY channel NULLS LAST, channel_or_unknown NULLS LAST
