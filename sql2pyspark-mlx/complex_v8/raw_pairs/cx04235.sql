WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        DATE_TRUNC('DAY', opened_at) AS opened_at_day
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 6 AND 12
      AND priority = 'P3'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM prepared_tickets s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('escalated', 'created')
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    priority,
    GROUPING(agent_id) AS is_total_agent_id,
    COUNT(*) FILTER (WHERE channel IN ('chat', 'web', 'email')) AS n_channel_in_chat_web_email
FROM exploded_detail_events
GROUP BY GROUPING SETS ((agent_id, priority), (agent_id))
ORDER BY agent_id NULLS LAST, priority NULLS LAST
