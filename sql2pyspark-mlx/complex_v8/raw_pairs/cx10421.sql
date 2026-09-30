WITH recent AS (
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
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 6 AND 10
      AND requester_id <= 18
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        SUM(csat) AS total_ticket_csat,
        MAX(closed_at) AS last_ticket_closed_at,
        MAX(closed_at) AS last_ticket_closed_at_2
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.total_ticket_csat,
        t.last_ticket_closed_at,
        t.last_ticket_closed_at_2
    FROM recent s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.channel_len,
        s.total_ticket_csat,
        s.last_ticket_closed_at_2,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM summarized_rows s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN actor NOT IN ('customer', 'agent') THEN 1 END) AS event_actor_count,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE actor <> 'agent') AS event_actor_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows_2 AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count_2, 0) AS event_actor_count_2
    FROM exploded_detail s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
-- Final projection
SELECT DISTINCT channel
FROM summarized_rows_2
ORDER BY channel DESC NULLS FIRST
