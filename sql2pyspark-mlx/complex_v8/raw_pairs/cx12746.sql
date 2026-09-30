WITH unioned AS (
    -- Stack the branches
    SELECT 'ticket' AS origin, requester_id, priority AS name
    FROM tickets
    UNION ALL
    SELECT 'agent' AS origin, id, tier
    FROM support_agents
)
SELECT origin, COUNT(*) AS n_rows, SUM(requester_id) AS total_requester_id
FROM unioned
GROUP BY origin
ORDER BY origin
