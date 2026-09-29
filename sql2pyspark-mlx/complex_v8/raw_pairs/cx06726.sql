WITH combined AS (
    -- Stack the branches
    SELECT 'ticket' AS source, requester_id AS key_id, opened_at AS happened_at
    FROM tickets
    UNION ALL
    SELECT 'ticket_2' AS source, agent_id, closed_at
    FROM tickets
)
SELECT source, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY source
ORDER BY source
