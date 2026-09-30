WITH filtered AS (
    -- Filter tickets
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
        CONCAT_WS('|', channel, priority) AS channel_priority_label,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE channel = 'web'
      AND agent_id >= 7
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.tags_count,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(csat) OVER w_event_id_channel_by_tags_cou AS next_csat,
        ROUND(tags_count - AVG(tags_count) OVER (PARTITION BY agent_id), 2) AS tags_count_vs_agent_id_avg,
        MIN(csat) OVER (PARTITION BY agent_id) AS agent_id_min_csat
    FROM with_children
    WINDOW w_event_id_channel_by_tags_cou AS (PARTITION BY event_id, channel ORDER BY tags_count DESC, id, event_id)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM sequenced s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    channel,
    tags,
    tags_count,
    event_id,
    event_type,
    next_csat,
    tags_count_vs_agent_id_avg,
    ticket_count
FROM with_counts
ORDER BY priority
