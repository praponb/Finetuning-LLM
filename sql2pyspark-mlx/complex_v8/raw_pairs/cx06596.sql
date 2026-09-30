WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, t.opened_at AS ticket_opened_at, t.tags AS ticket_tags
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT event_at, event_type, actor, event_at_year
    FROM with_parent
) src
PIVOT (
    MIN(event_at_year)
    FOR actor IN ('customer', 'system', 'agent')
)
ORDER BY event_at, event_type
