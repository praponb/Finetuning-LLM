WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        tags
    FROM tickets
),
unioned AS (
    -- Stack the branches
    SELECT 'ticket' AS source, id AS key_id, agent_id
    FROM recent
    WHERE MONTH(closed_at) IN (1, 3)
    UNION
    SELECT 'event' AS source, id, ticket_id
    FROM ticket_events
    WHERE ticket_id >= 43
)
SELECT source, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM unioned
GROUP BY source
ORDER BY source
