WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE actor IN ('customer', 'system')
      AND ticket_id IS NOT NULL
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_clean,
        t.priority AS ticket_priority
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.priority IS NOT NULL
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM combined_details_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 10:00:00'
              AND t.actor IS NOT NULL
        )
      AND s.event_type_clean IS NOT NULL
)
SELECT
    id,
    event_at,
    event_type_clean,
    ticket_priority,
    CONCAT_WS(': ', ticket_priority, event_type_clean) AS ticket_priority_event_type_clean_label,
    LENGTH(TRIM(ticket_priority)) AS ticket_priority_len
FROM qualified
WHERE ticket_priority NOT IN ('P2', 'P4')
ORDER BY event_type_clean, event_at
