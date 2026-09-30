WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
stacked AS (
    -- Stack the branches
    SELECT 'event' AS origin, ticket_id AS qty, id AS key_id
    FROM cleaned
    WHERE event_type = 'assigned'
      AND TO_DATE(event_at) < DATE '2025-02-12'
    UNION ALL
    SELECT 'event_2' AS origin, ticket_id AS qty, id AS key_id
    FROM ticket_events
)
SELECT origin, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM stacked
GROUP BY origin
ORDER BY origin
