WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    ticket_channel,
    ticket_closed_at,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    CONCAT_WS('|', actor, event_type) AS actor_event_type_label
FROM joined
ORDER BY event_type_or_unknown NULLS LAST, id
LIMIT 20
