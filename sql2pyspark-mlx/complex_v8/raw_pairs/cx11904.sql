WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE ticket_id <= 38
)
SELECT 'event' AS origin, ticket_id AS qty, event_at
FROM cleaned
WHERE event_at_dayofweek > 3
UNION
SELECT 'ticket' AS origin, csat, opened_at
FROM tickets
WHERE priority IN ('P3', 'P2', 'P1')
ORDER BY origin
