WITH all_events AS (
    -- Stack the branches
    SELECT 'event' AS record_type, ticket_id AS qty, event_at AS event_time
    FROM ticket_events
    UNION ALL
    SELECT 'event_2' AS record_type, ticket_id, event_at
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_at < TIMESTAMP '2025-02-12 15:00:00'
)
SELECT record_type, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY record_type
ORDER BY record_type
