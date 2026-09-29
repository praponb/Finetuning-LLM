WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM base_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'customer'
)
SELECT
    event_event_type,
    ticket_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count
FROM with_children_events
GROUP BY CUBE(event_event_type, ticket_id)
ORDER BY event_event_type NULLS LAST, ticket_id NULLS LAST
