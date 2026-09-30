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
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM matched s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND TO_DATE(t.closed_at) < DATE '2025-02-08'
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM exploded_detail_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'created'
        )
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.ticket_id_2,
        s.ticket_closed_at,
        s.ticket_priority,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at_2,
        t.tags AS ticket_tags
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    ticket_closed_at,
    ticket_priority,
    ticket_tags,
    ARRAY_JOIN(ARRAY_SORT(ticket_tags), ',') AS ticket_tags_text
FROM combined_details_ticket
WHERE ARRAY_CONTAINS(ticket_tags, 'bug')
