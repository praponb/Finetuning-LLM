WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE ticket_id >= 23
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM cleaned s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type IN ('reopened', 'assigned')
              AND t.event_at >= TIMESTAMP '2025-01-25 08:00:00'
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor
FROM filtered
ORDER BY event_at, actor DESC NULLS FIRST, id
LIMIT 5
