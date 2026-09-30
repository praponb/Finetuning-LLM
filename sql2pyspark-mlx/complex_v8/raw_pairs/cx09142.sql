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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        t.id AS ticket_id_2,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags
    FROM matched s
    JOIN tickets t ON t.agent_id = s.agent_id AND t.priority IN ('P1', 'P3')
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    ticket_priority,
    ticket_tags,
    SUBSTRING(TRIM(ticket_priority), 1, 3) AS ticket_priority_prefix,
    LOWER(REGEXP_REPLACE(TRIM(ticket_channel), ' ', '_')) AS ticket_channel_slug
FROM detail_rows_tickets
