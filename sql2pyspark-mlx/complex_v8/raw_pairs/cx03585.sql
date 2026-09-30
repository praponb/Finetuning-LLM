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
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS ticket_id_2,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel
    FROM matched s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND t.channel IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        priority,
        ticket_id_2,
        ticket_opened_at,
        ticket_channel,
        CONCAT(ticket_channel, '#', CAST(ticket_id_2 AS STRING)) AS ticket_channel_ref,
        CONCAT_WS('-', ticket_channel, priority) AS ticket_channel_priority_label
    FROM detail_rows_tickets
)
-- Final projection
SELECT
    id,
    ticket_id_2,
    ticket_opened_at,
    ticket_channel,
    ticket_channel_ref,
    ticket_channel_priority_label,
    LOWER(REGEXP_REPLACE(TRIM(ticket_channel_ref), ' ', '_')) AS ticket_channel_ref_slug,
    CASE WHEN priority IN ('P3', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
FROM derived
WHERE ticket_id_2 <= 91
