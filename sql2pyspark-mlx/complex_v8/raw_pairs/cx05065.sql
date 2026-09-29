WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
)
SELECT id
FROM base_events
WHERE TO_DATE(event_at) < DATE '2025-02-18'
  AND actor NOT IN ('customer', 'system')
EXCEPT
SELECT agent_id AS id
FROM tickets
ORDER BY id
