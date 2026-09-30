WITH combined AS (
    -- Stack the branches
    SELECT id AS key_id, csat AS qty
    FROM tickets
    UNION ALL
    SELECT id, csat
    FROM tickets
    WHERE requester_id BETWEEN 5 AND 19
    UNION ALL
    SELECT id, ticket_id
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
)
SELECT key_id, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM combined
GROUP BY key_id
ORDER BY key_id
