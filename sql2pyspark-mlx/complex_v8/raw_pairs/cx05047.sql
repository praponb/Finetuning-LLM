WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
      AND ticket_id >= 50
)
SELECT ticket_id, event_type AS name
FROM cleaned_events
WHERE event_type IN ('assigned', 'reopened')
  AND actor = 'customer'
EXCEPT
SELECT requester_id AS ticket_id, priority AS name
FROM tickets
WHERE HOUR(closed_at) BETWEEN 10 AND 15
  AND priority = 'P2'
ORDER BY ticket_id
