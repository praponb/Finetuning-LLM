WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE actor = 'system'
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_slug,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM prepared_events s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_id,
        s.event_event_type,
        t.id AS event_id_2,
        t.event_at,
        t.actor AS event_actor_2,
        t.event_type AS event_event_type_2
    FROM line_level_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        COUNT(CASE WHEN actor IS NOT NULL THEN 1 END) AS event_actor_count,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        t.first_event_at_2
    FROM with_children s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT DISTINCT event_event_type_2, id, event_at
FROM with_stats
WHERE event_id IS NOT NULL
ORDER BY event_event_type_2 DESC, id
