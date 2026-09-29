WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM cleaned s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND HOUR(t.event_at) BETWEEN 10 AND 14
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM with_children_events s
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_id,
    event_event_at,
    event_event_type,
    event_count,
    COALESCE(event_count, 0) AS event_count_filled
FROM profiled
ORDER BY event_event_at DESC NULLS FIRST
