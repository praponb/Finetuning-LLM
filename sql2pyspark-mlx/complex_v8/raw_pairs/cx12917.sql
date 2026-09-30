WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d
    FROM ticket_events
    WHERE actor = 'system'
      AND event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM prepared_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM exploded_detail_events s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor IS NOT NULL
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM qualified s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2
    FROM profiled s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        actor,
        event_id,
        event_event_at,
        event_actor,
        event_event_type,
        last_event_at,
        first_event_at,
        first_event_at_2,
        CONCAT(event_event_type, '#', CAST(ticket_id AS STRING)) AS event_event_type_ref,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM summarized_rows
    WHERE event_event_type IS NOT NULL
)
-- Final projection
SELECT
    id,
    actor,
    event_actor,
    event_event_type,
    first_event_at_2,
    event_event_type_ref,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
    COALESCE(event_at_dayofweek, 0) AS event_at_dayofweek_filled
FROM annotated
