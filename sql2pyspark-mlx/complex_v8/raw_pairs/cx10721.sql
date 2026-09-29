WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE ticket_id >= 11
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_dayofweek,
        s.event_at_label,
        s.event_type_clean,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM scoped_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IS NOT NULL
)
-- Final projection
SELECT
    id,
    event_at,
    event_at_dayofweek,
    event_at_label,
    event_type_clean,
    event_id,
    event_event_type,
    event_event_at,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
FROM line_level
ORDER BY event_event_at DESC NULLS FIRST
