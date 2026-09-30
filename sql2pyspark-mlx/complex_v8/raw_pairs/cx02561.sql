WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM joined_ranges s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        event_event_type,
        event_id,
        TO_DATE(event_event_at) AS event_event_at_day,
        ROUND(100 * COUNT(CASE WHEN priority IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_priority_is_not_null,
        COUNT(DISTINCT event_event_type) AS distinct_event_event_types
    FROM exploded_detail
    GROUP BY event_event_type, event_id, TO_DATE(event_event_at)
)
SELECT
    event_event_type,
    event_event_at_day,
    pct_priority_is_not_null,
    distinct_event_event_types,
    DATEDIFF(DATE '2025-12-31', event_event_at_day) AS days_since_event_event_at_day,
    ABS(pct_priority_is_not_null - 50.0) AS pct_priority_is_not_null_distance
FROM grouped
ORDER BY distinct_event_event_types DESC, event_event_type
