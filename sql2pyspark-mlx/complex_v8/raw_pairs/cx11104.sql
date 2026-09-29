WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-12'
),
all_events AS (
    -- Stack the branches
    SELECT event_type, event_at AS event_time, 'event' AS source
    FROM valid
    WHERE event_type = 'escalated'
    UNION
    SELECT actor, event_at, 'event_2' AS source
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 15
      AND ticket_id IS NOT NULL
)
SELECT source, COUNT(*) AS n_rows, MAX(event_time) AS latest_event_time
FROM all_events
GROUP BY source
ORDER BY source
