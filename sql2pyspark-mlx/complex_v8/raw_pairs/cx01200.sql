WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
)
SELECT ticket_id
FROM recent_events
EXCEPT
SELECT agent_id AS ticket_id
FROM tickets
WHERE csat < 5
ORDER BY ticket_id DESC
