WITH all_events AS (
    -- Stack the branches
    SELECT 'event' AS src, ticket_id AS ref_id, actor AS category
    FROM ticket_events
    WHERE event_type IN ('assigned', 'created')
    UNION
    SELECT 'agent' AS src, id AS ref_id, team AS category
    FROM support_agents
)
SELECT src, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM all_events
GROUP BY src
ORDER BY src
