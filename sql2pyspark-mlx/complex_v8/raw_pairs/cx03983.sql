WITH stacked AS (
    -- Stack the branches
    SELECT
        'ticket' AS record_type,
        csat,
        priority,
        closed_at AS happened_at
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-26 16:00:00'
    UNION
    SELECT
        'event' AS record_type,
        id AS csat,
        actor AS priority,
        event_at AS happened_at
    FROM ticket_events
    WHERE event_type IN ('resolved', 'replied')
)
SELECT record_type, COUNT(*) AS n_rows, SUM(csat) AS total_csat, MAX(happened_at) AS latest_happened_at
FROM stacked
GROUP BY record_type
ORDER BY record_type
