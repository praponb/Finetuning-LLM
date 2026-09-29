WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-02'
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.opened_at AS ticket_opened_at
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    ticket_opened_at,
    CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
    DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
FROM with_parent_ticket
WHERE actor = 'system'
ORDER BY actor DESC, id
