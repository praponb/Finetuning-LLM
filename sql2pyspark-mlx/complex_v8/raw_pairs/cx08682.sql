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
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        t.opened_at AS ticket_opened_at
    FROM covered s
    JOIN tickets t ON t.id = s.ticket_id
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.agent_id,
        s.hours_since_open,
        s.ticket_opened_at,
        t.csat AS ticket_csat,
        su.tier AS agent_tier
    FROM with_details s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.agent_id,
        s.hours_since_open,
        s.agent_tier,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier_2,
        su.agent_name
    FROM with_parent s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    hours_since_open,
    agent_tier,
    agent_name,
    DATEDIFF(DATE '2026-01-01', agent_hired_on) AS days_since_agent_hired_on
FROM joined_agent
ORDER BY agent_id
