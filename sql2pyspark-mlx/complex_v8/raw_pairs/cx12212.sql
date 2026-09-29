WITH joined_ranges AS (
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
scored AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        hours_since_open,
        CASE WHEN hours_since_open >= 50.4 THEN 'A' WHEN hours_since_open >= 28.8 THEN 'B' ELSE 'C' END AS hours_since_open_tier
    FROM joined_ranges
    WHERE ticket_id IS NOT NULL
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.hours_since_open,
        s.hours_since_open_tier,
        t.csat AS ticket_csat,
        su.agent_name,
        su.team AS agent_team
    FROM scored s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    ticket_csat,
    agent_team
FROM with_details_ticket
WHERE UPPER(TRIM(agent_name)) LIKE 'H%'
ORDER BY event_at DESC, event_type DESC
