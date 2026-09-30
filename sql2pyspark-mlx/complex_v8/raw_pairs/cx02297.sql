WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE actor = 'customer'
),
event_type_stats AS (
    -- Avg event_at_hour per event_type
    SELECT event_type, AVG(event_at_hour) AS avg_event_at_hour
    FROM base
    GROUP BY event_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_hour,
        ROUND(s.event_at_hour - g.avg_event_at_hour, 2) AS event_at_hour_minus_avg
    FROM base s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_at_hour > g.avg_event_at_hour
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM above_average s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_parent_ticket_2 AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_prefix,
        s.event_at_hour,
        s.event_at_hour_minus_avg,
        s.ticket_tags,
        s.ticket_csat,
        s.agent_tier,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel
    FROM with_parent_ticket s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.priority IS NOT NULL
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_prefix,
        s.event_at_hour,
        s.event_at_hour_minus_avg,
        s.ticket_csat,
        s.agent_tier,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM with_parent_ticket_2 s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_at_hour,
    event_at_hour_minus_avg,
    ticket_csat,
    agent_tier,
    event_id,
    event_event_at,
    event_event_type,
    GREATEST(ticket_csat, event_at_hour_minus_avg) AS greatest_ticket_csat_event_at_hour_minus_avg,
    SUBSTRING(TRIM(actor_prefix), 1, 3) AS actor_prefix_prefix
FROM line_level_events
ORDER BY event_at_hour_minus_avg DESC, actor_prefix_prefix DESC
