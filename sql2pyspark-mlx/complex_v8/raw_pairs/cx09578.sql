WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'replied'
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.closed_at AS ticket_closed_at
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(actor)) AS actor_len,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM with_parent
    WHERE actor = 'system'
)
SELECT
    id,
    ticket_id,
    actor,
    ticket_closed_at,
    actor_len,
    event_at_label
FROM annotated
WHERE actor IN ('customer', 'agent')
ORDER BY event_at_label DESC NULLS FIRST, id
LIMIT 10
