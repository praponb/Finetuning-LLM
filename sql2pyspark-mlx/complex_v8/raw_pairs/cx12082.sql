WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('escalated', 'created')
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM cleaned s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_id,
        s.event_event_at,
        t.id AS event_id_2,
        t.event_at,
        t.event_type AS event_event_type_2
    FROM with_children s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_id,
        s.event_event_at,
        s.event_id_2,
        s.event_at,
        s.event_event_type_2,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM exploded_detail s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel <> 'phone'
),
combined_details_event AS (
    -- Join ticket_events
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_id_2,
        s.event_at,
        s.event_event_type_2,
        t.ticket_id AS event_ticket_id
    FROM with_parent_ticket s
    JOIN ticket_events t ON t.id = s.event_id_2
    WHERE t.ticket_id IS NOT NULL
)
SELECT
    id,
    ticket_id,
    event_id,
    event_id_2,
    event_at
FROM combined_details_event
WHERE event_id_2 IS NOT NULL
ORDER BY event_id_2 NULLS LAST, ticket_id NULLS LAST, id, event_id
LIMIT 5
