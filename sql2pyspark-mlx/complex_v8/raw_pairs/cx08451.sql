WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND ticket_id <= 35
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
)
-- Final projection
SELECT DISTINCT id, event_at
FROM without_matches
ORDER BY event_at DESC
