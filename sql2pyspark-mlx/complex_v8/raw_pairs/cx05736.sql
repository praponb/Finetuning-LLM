WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE actor = 'customer'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type_actor_label,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM valid_events
    WHERE actor = 'customer'
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_actor_label,
        s.actor_slug,
        s.actor_ref,
        t.closed_at AS ticket_closed_at
    FROM labeled s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type_actor_label,
    actor_slug,
    ticket_closed_at,
    DATE_TRUNC('DAY', ticket_closed_at) AS ticket_closed_at_day
FROM enriched
