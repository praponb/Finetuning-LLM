WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE ticket_id >= 16
      AND event_type IS NOT NULL
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_date,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM base_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IN ('escalated', 'created', 'replied')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_id,
        event_event_type,
        event_event_at,
        DATEDIFF(event_at_date, LAG(event_at_date) OVER w_all_by_event_at_date) AS days_since_prev_event_at_date
    FROM exploded_detail
    WINDOW w_all_by_event_at_date AS (ORDER BY event_at_date DESC NULLS FIRST, id, event_id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_id,
    days_since_prev_event_at_date,
    ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(event_event_at)) / 60, 1) AS minutes_event_event_at_to_event_at,
    DATEDIFF(DATE '2025-06-30', event_event_at) AS days_since_event_event_at
FROM with_windows
WHERE event_event_type IN ('reopened', 'escalated', 'resolved')
ORDER BY event_id DESC, id
LIMIT 5
