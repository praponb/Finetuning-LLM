WITH all_events AS (
    -- Stack the branches
    SELECT 'event' AS origin, ticket_id, id
    FROM ticket_events
    WHERE event_type = 'reopened'
      AND HOUR(event_at) BETWEEN 6 AND 10
    UNION ALL
    SELECT 'ticket' AS origin, csat, requester_id
    FROM tickets
    UNION ALL
    SELECT 'ticket' AS origin, agent_id, csat
    FROM tickets
)
SELECT origin, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM all_events
GROUP BY origin
ORDER BY origin
