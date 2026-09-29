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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        su.team AS agent_team,
        su.agent_name,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    ticket_id,
    event_type,
    ticket_priority,
    ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
    SUM(CASE WHEN LENGTH(TRIM(agent_name)) >= 4 THEN hours_since_open ELSE 0 END) AS hours_since_open_length_trim_agent_name_4
FROM with_details
GROUP BY ROLLUP(ticket_id, event_type, ticket_priority)
