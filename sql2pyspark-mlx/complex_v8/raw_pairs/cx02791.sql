WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 37
)
SELECT ticket_id, event_at
FROM scoped_events
WHERE actor IS NOT NULL
UNION ALL
SELECT id, closed_at
FROM tickets
WHERE TO_DATE(opened_at) < DATE '2025-02-18'
ORDER BY event_at
