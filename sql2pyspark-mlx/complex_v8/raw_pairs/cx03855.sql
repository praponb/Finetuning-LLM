WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'reopened'
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM prepared s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(event_actor, 'UNKNOWN') AS event_actor_or_unknown,
        LENGTH(TRIM(event_actor)) AS event_actor_len
    FROM line_level_events
),
with_details_event AS (
    -- Attach event attributes
    SELECT s.*, t.ticket_id AS event_ticket_id
    FROM scored s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.ticket_id IS NOT NULL
)
-- Subtotals with CUBE
SELECT
    COALESCE(event_actor, 'ALL') AS event_actor,
    COALESCE(event_actor_or_unknown, 'ALL') AS event_actor_or_unknown,
    SUM(event_actor_len) AS total_event_actor_len
FROM with_details_event
GROUP BY CUBE(event_actor, event_actor_or_unknown)
ORDER BY event_actor NULLS LAST, event_actor_or_unknown NULLS LAST
