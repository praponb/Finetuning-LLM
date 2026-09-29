WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE ticket_id <= 50
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_ref,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
),
latest AS (
    -- Keep the earliest row per ticket_id, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        ticket_requester_id,
        ticket_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY ticket_closed_at, id) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type, actor, ticket_requester_id
    FROM latest
) src
PIVOT (
    MAX(ticket_requester_id)
    FOR actor IN ('agent', 'system', 'customer')
)
ORDER BY event_type
