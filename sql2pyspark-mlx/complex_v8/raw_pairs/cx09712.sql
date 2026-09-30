WITH stacked AS (
    -- Stack the branches
    SELECT requester_id, csat AS key_id
    FROM tickets
    WHERE requester_id BETWEEN 5 AND 22
    UNION
    SELECT id AS requester_id, ticket_id AS key_id
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 9 AND 14
      AND event_type IS NOT NULL
)
SELECT requester_id, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM stacked
GROUP BY requester_id
ORDER BY requester_id
