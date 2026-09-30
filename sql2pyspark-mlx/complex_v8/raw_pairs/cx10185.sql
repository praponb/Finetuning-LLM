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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.agent_id,
        s.hours_since_open,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM covered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT *
FROM (
    SELECT agent_id, agent_tier, hours_since_open
    FROM with_details_agent
) src
PIVOT (
    MAX(hours_since_open)
    FOR agent_tier IN ('T3', 'T2')
)
