WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 12
)
SELECT ticket_id AS entity_id
FROM valid
WHERE event_at < TIMESTAMP '2025-02-12 09:00:00'
  AND ticket_id >= 17
EXCEPT
SELECT id AS entity_id
FROM ticket_events
ORDER BY entity_id
