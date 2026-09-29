WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE event_type = 'created'
      AND actor <> 'system'
)
SELECT event_type AS category
FROM recent
MINUS
SELECT priority
FROM tickets
