WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id >= 7
      AND event_type = 'resolved'
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_clean,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM cleaned_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor IN ('customer', 'agent')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        actor_clean,
        event_id,
        event_actor,
        event_event_at,
        LENGTH(TRIM(actor_clean)) AS actor_clean_len
    FROM line_level
    WHERE ticket_id IS NOT NULL
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_id,
        s.event_actor,
        t.id AS event_id_2,
        t.actor AS event_actor_2,
        t.event_type AS event_event_type
    FROM scored s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_id,
    event_actor_2,
    event_event_type,
    LENGTH(TRIM(event_type)) AS event_type_len,
    COALESCE(event_actor_2, 'UNKNOWN') AS event_actor_2_or_unknown
FROM exploded_detail
ORDER BY event_actor_2_or_unknown NULLS LAST
