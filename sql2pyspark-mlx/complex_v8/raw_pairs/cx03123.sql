WITH prepared_tickets AS (
    -- Scope the tickets rows
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
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE channel NOT IN ('email', 'phone')
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.hired_on AS agent_hired_on
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on < DATE '2024-12-31'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.actor IS NOT NULL THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM combined_details_agent s
),
channel_stats AS (
    -- Max event_actor_count per channel
    SELECT channel, MAX(event_actor_count) AS max_event_actor_count
    FROM with_counts
    GROUP BY channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_actor_count,
        ROUND(s.event_actor_count - g.max_event_actor_count, 2) AS event_actor_count_minus_max
    FROM with_counts s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.event_actor_count = g.max_event_actor_count
),
combined_details_agent_2 AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.agent_hired_on,
        s.event_actor_count,
        s.event_actor_count_minus_max,
        su.agent_name
    FROM compared s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    channel,
    csat,
    tags,
    event_actor_count,
    event_actor_count_minus_max,
    agent_name,
    COALESCE(event_actor_count, 0) AS event_actor_count_filled,
    ABS(csat - 3) AS csat_distance
FROM combined_details_agent_2
ORDER BY event_actor_count_filled DESC, agent_id DESC, id
LIMIT 20
