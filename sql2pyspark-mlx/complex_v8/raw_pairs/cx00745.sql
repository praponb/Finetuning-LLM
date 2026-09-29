WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        tags,
        CASE WHEN priority IN ('P4', 'P3') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE csat <= 4
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(priority_group) OVER (PARTITION BY channel, agent_id)) AS distinct_priority_group_in_channel_agent_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel, agent_id), 0), 4) AS requester_id_share_of_channel_agent_id
    FROM recent
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.tags,
        s.priority_group,
        s.requester_id_share_of_channel_agent_id,
        su.team AS agent_team,
        su.agent_name
    FROM ranked s
    JOIN support_agents su ON su.id = s.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2,
        COUNT(*) FILTER (WHERE event_type NOT IN ('created', 'escalated')) AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM with_parent_agent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT *
FROM (
    SELECT last_event_at_2, priority_group, requester_id
    FROM with_stats
) src
PIVOT (
    COUNT(requester_id)
    FOR priority_group IN ('other', 'group_b', 'group_a')
)
ORDER BY last_event_at_2
