WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_csat,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM combined_details_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    event_at,
    ticket_csat,
    event_id,
    event_actor
FROM detail_rows
