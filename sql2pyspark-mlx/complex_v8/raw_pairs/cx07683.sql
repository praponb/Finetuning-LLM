WITH in_window AS (
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
enriched AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM in_window
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.days_since_event_at,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM enriched s
    JOIN tickets t ON t.agent_id = s.agent_id
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(ticket_closed_at, 'MMM yyyy') AS ticket_closed_at_label
    FROM detail_rows
)
-- Final projection
SELECT
    id,
    event_at,
    priority,
    agent_id,
    days_since_event_at,
    ticket_id_2,
    ticket_closed_at,
    ticket_channel,
    ticket_closed_at_label,
    CONCAT_WS('-', ticket_channel, priority) AS ticket_channel_priority_label
FROM scored
ORDER BY ticket_channel DESC, ticket_channel_priority_label DESC
