WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE actor = 'system'
      AND event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 8 AND 13
        )
)
-- Final projection
SELECT id, ticket_id, event_at
FROM with_activity
ORDER BY ticket_id DESC NULLS FIRST
