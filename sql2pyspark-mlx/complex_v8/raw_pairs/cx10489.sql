WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('system', 'agent')
      AND MONTH(event_at) IN (1, 3)
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM scoped_events s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE HOUR(t.event_at) BETWEEN 6 AND 12
              AND t.actor NOT IN ('customer', 'system')
        )
      AND MONTH(s.event_at) IN (1, 3)
)
SELECT id, event_at, event_type
FROM screened
ORDER BY id NULLS LAST, event_at DESC
