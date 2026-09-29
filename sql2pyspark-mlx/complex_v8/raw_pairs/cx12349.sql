WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_type = 'escalated'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(event_at_dayofweek, actor_len) AS greatest_event_at_dayofweek_actor_len
    FROM prepared_events
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.greatest_event_at_dayofweek_actor_len,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM exploded_detail_events
    WHERE greatest_event_at_dayofweek_actor_len < (SELECT MAX(greatest_event_at_dayofweek_actor_len) FROM exploded_detail_events)
)
SELECT
    id,
    ticket_id,
    event_at,
    greatest_event_at_dayofweek_actor_len,
    event_id,
    event_event_at,
    event_event_type,
    event_actor,
    COALESCE(event_actor, 'UNKNOWN') AS event_actor_or_unknown
FROM screened
ORDER BY event_event_at, event_actor
