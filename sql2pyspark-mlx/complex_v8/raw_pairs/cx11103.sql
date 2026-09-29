WITH combined AS (
    -- Stack the branches
    SELECT
        'event' AS src,
        ticket_id AS ref_id,
        id AS key_id,
        event_at
    FROM ticket_events
    UNION
    SELECT
        'ticket' AS src,
        id AS ref_id,
        agent_id AS key_id,
        closed_at AS event_at
    FROM tickets
)
SELECT src, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(event_at) AS latest_event_at
FROM combined
GROUP BY src
ORDER BY src
