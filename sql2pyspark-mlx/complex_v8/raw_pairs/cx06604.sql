WITH cleaned AS (
    -- Keep valid rows and derive helper columns
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
        LENGTH(TRIM(priority)) AS priority_len,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE priority = 'P3'
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.priority_len,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on < DATE '2024-12-31'
),
latest AS (
    -- Keep the latest row per priority, channel
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        priority_len,
        agent_tier,
        agent_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, channel ORDER BY csat DESC, id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel,
    csat,
    priority_len,
    agent_team
FROM latest
ORDER BY id
LIMIT 20
