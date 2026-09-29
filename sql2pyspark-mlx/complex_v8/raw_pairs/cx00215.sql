WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags,
        su.team AS agent_team
    FROM covered s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = s.agent_id
    WHERE ARRAY_CONTAINS(t.tags, 'login')
),
with_details_ticket_2 AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.agent_id,
        s.hours_since_open,
        s.ticket_csat,
        s.ticket_opened_at,
        s.ticket_tags,
        s.agent_team,
        t.opened_at AS ticket_opened_at_2,
        t.priority AS ticket_priority,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM with_details_ticket s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = s.agent_id
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.agent_id,
        s.ticket_csat,
        s.ticket_opened_at,
        s.ticket_opened_at_2,
        s.agent_hired_on,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type
    FROM with_details_ticket_2 s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'agent'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    agent_id,
    ticket_csat,
    ticket_opened_at_2,
    agent_hired_on,
    event_id,
    event_event_at,
    event_actor,
    YEAR(event_event_at) AS event_event_at_year
FROM line_level
WHERE agent_id IS NOT NULL
ORDER BY agent_id NULLS LAST
