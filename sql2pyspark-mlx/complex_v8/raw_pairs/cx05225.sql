WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE event_type <> 'assigned'
      AND TO_DATE(event_at) < DATE '2025-02-24'
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type = 'assigned') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM recent_events s
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_event_type_count,
        ABS(event_count - 3) AS event_count_distance
    FROM summarized_rows
    WHERE last_event_at >= TIMESTAMP '2025-01-17 09:00:00'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_count_distance,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM labeled s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    event_at,
    event_count_distance,
    event_id,
    event_actor,
    event_event_type,
    ABS(event_count_distance - 4) AS event_count_distance_distance,
    DATE_TRUNC('DAY', event_at) AS event_at_day
FROM exploded_detail_events
ORDER BY event_id DESC
