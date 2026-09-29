WITH all_events AS (
    -- Stack the branches
    SELECT
        'event' AS origin,
        id AS ref_id,
        event_at,
        event_type AS label
    FROM ticket_events
    UNION ALL
    SELECT
        'event_2' AS origin,
        id,
        event_at,
        event_type
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
)
SELECT origin, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(event_at) AS latest_event_at
FROM all_events
GROUP BY origin
ORDER BY origin
