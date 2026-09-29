WITH all_events AS (
    -- Stack the branches
    SELECT
        priority AS name,
        opened_at,
        agent_id AS qty,
        'ticket' AS source
    FROM tickets
    WHERE channel NOT IN ('web', 'chat')
    UNION ALL
    SELECT
        event_type,
        event_at,
        id,
        'event' AS source
    FROM ticket_events
)
SELECT source, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(opened_at) AS latest_opened_at
FROM all_events
GROUP BY source
ORDER BY source
