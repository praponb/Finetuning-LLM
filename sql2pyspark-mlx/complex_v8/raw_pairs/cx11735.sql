WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (event_type IN ('created', 'reopened', 'escalated') OR ticket_id >= 8)
)
SELECT id
FROM prepared_events
INTERSECT
SELECT ticket_id
FROM ticket_events
ORDER BY id
