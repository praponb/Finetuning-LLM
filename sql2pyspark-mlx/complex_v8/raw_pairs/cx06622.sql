WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN requester_id >= 18 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        CASE WHEN channel IN ('web', 'email') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 8 AND 12
      AND agent_id >= 8
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.greatest_requester_id_csat,
        s.channel_group,
        su.tier AS agent_tier
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM with_details_agent
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(channel_group), ' ', '_')) AS channel_group_slug,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM scored
    WHERE channel_or_unknown = 'email'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(closed_at) AS first_ticket_closed_at,
        MAX(opened_at) AS last_ticket_opened_at,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at,
        t.last_ticket_opened_at,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM tagged s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    csat,
    tags,
    channel_group,
    channel_or_unknown,
    channel_group_slug,
    priority_slug,
    last_ticket_opened_at,
    ticket_count,
    SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
    SIZE(tags) AS tags_count
FROM with_counts
ORDER BY channel_group NULLS LAST, closed_at
