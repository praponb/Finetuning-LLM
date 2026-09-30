WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE actor IN ('customer', 'system')
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'system'
              AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
        )
      AND s.event_at_label IS NOT NULL
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_event_type_label,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND TO_DATE(t.event_at) < DATE '2025-03-14'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_event_type_label,
    event_id,
    event_event_at,
    event_actor,
    event_event_type,
    TO_DATE(event_event_at) AS event_event_at_date
FROM with_children_events
WHERE actor_event_type_label IS NOT NULL
