WITH matched AS (
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
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.agent_id,
        s.hours_since_open,
        su.tier AS agent_tier,
        su.agent_name
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    agent_id,
    hours_since_open,
    agent_name,
    LENGTH(TRIM(agent_tier)) AS agent_tier_len,
    ABS(hours_since_open - 25.2) AS hours_since_open_distance
FROM joined
WHERE hours_since_open > 14.4
