WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 11:00:00'
      AND ticket_id IS NOT NULL
)
SELECT ticket_id
FROM recent
WHERE actor IN ('agent', 'customer')
EXCEPT
SELECT id
FROM ticket_events
WHERE ticket_id >= 13
  AND event_at >= TIMESTAMP '2025-01-25 09:00:00'
ORDER BY ticket_id DESC
