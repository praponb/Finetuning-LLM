WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS('|', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE requester_id >= 11
),
joined_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM recent s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'VIP'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM joined_agent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    tags,
    priority_channel_label,
    agent_name,
    agent_team,
    event_count,
    event_count_2
FROM profiled
ORDER BY csat NULLS LAST, channel, id
LIMIT 10
