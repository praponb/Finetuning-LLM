WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND TO_DATE(event_at) < DATE '2025-02-06'
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM combined_details_ticket
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM enriched
ORDER BY event_type_len DESC, id
LIMIT 10
