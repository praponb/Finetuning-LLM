WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-14'
)
SELECT event_type, actor AS category
FROM base
WHERE event_type = 'assigned'
  AND HOUR(event_at) BETWEEN 9 AND 15
UNION ALL
SELECT event_type, actor
FROM ticket_events
WHERE HOUR(event_at) BETWEEN 9 AND 15
  AND event_type = 'created'
ORDER BY category DESC, event_type
