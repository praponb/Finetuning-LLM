WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
    FROM ticket_events
    WHERE actor <> 'customer'
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM base s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
labeled AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(event_event_type)) AS event_event_type_len
    FROM detail_rows
    WHERE event_type NOT IN ('assigned', 'resolved')
)
SELECT DISTINCT event_type, event_id
FROM labeled
WHERE ticket_id >= 40
ORDER BY event_id DESC
