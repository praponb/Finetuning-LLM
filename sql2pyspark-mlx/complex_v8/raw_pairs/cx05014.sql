WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type IN ('replied', 'reopened')
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_week,
        s.actor_or_unknown,
        t.tags AS ticket_tags
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_week,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM with_details
    WHERE TO_DATE(event_at_week) < DATE '2025-02-09'
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_week,
    event_type_clean
FROM enriched
