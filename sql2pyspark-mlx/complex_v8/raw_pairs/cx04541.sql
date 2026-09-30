WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label_2
    FROM ticket_events
    WHERE actor = 'customer'
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        ABS(actor_len - 7) AS actor_len_distance
    FROM scoped
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_len,
        s.actor_event_type_label,
        s.days_since_event_at,
        s.event_type_or_unknown,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor_len,
    actor_event_type_label,
    event_type_or_unknown,
    ticket_priority,
    ticket_closed_at,
    UPPER(TRIM(event_type_or_unknown)) AS event_type_or_unknown_clean
FROM with_parent_ticket
WHERE ticket_channel = 'web'
ORDER BY ticket_closed_at NULLS LAST, event_type_or_unknown_clean DESC, id
LIMIT 10
