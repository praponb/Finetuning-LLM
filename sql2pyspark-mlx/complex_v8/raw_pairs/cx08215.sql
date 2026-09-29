WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref
    FROM tickets
    WHERE agent_id >= 7
      AND closed_at < TIMESTAMP '2025-02-14 22:00:00'
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.tags,
        s.channel_ref,
        su.agent_name
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        channel,
        channel_ref,
        agent_name,
        ARRAY_CONTAINS(tags, 'bug') AS has_bug,
        LOWER(REGEXP_REPLACE(TRIM(channel_ref), ' ', '_')) AS channel_ref_slug,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM joined_agent
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
)
-- Final projection
SELECT
    id,
    channel,
    agent_name,
    channel_ref_slug,
    channel_slug,
    CONCAT(channel_slug, '#', CAST(id AS STRING)) AS channel_slug_ref,
    CONCAT_WS('|', agent_name, channel_ref) AS agent_name_channel_ref_label
FROM tagged
ORDER BY channel_ref_slug, id
