WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        INITCAP(TRIM(actor)) AS actor_clean,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE ticket_id <= 14
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_actor_label,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
),
with_details_ticket AS (
    -- Join tickets
    SELECT s.*, t.opened_at AS ticket_opened_at
    FROM joined s
    JOIN tickets t ON t.id = s.ticket_id
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_actor_label,
        s.ticket_closed_at,
        s.ticket_priority,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM with_details_ticket s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor <> 'system'
)
-- Subtotals with CUBE
SELECT
    ticket_id,
    event_type,
    SUM(CASE WHEN event_actor IS NOT NULL THEN 1 ELSE 0 END) AS n_event_actor_is_not_null,
    COUNT(*) FILTER (WHERE ticket_priority IS NOT NULL) AS n_ticket_priority_is_not_null,
    MIN(event_at) AS first_event_at
FROM with_children
GROUP BY CUBE(ticket_id, event_type)
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST
