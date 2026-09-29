WITH all_events AS (
    -- Stack the branches
    SELECT
        requester_id AS qty,
        agent_id,
        closed_at,
        'ticket' AS record_type
    FROM tickets
    UNION ALL
    SELECT
        ticket_id,
        id,
        event_at,
        'event' AS record_type
    FROM ticket_events
)
SELECT record_type, COUNT(*) AS n_rows, SUM(agent_id) AS total_agent_id, MAX(closed_at) AS latest_closed_at
FROM all_events
GROUP BY record_type
ORDER BY record_type
