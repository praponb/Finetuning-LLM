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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.channel AS ticket_channel,
        su.team AS agent_team,
        su.agent_name
    FROM covered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = s.agent_id
    WHERE t.channel IS NOT NULL
)
SELECT
    id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    ticket_channel,
    agent_team,
    agent_name
FROM with_details_ticket
WHERE event_type <> 'resolved'
ORDER BY ticket_channel DESC, event_type DESC
