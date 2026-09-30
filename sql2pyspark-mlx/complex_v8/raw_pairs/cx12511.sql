WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        CASE WHEN channel IN ('chat', 'email') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-02-04 23:00:00'
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.channel_group,
        su.agent_name,
        su.tier AS agent_tier
    FROM scoped s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        channel,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM joined_agent
)
-- Final projection
SELECT
    id,
    channel_slug,
    CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
    SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
FROM annotated
WHERE channel_slug IS NOT NULL
ORDER BY channel_ref, id
LIMIT 5
