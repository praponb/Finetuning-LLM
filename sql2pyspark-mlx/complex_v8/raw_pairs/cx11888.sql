WITH combined AS (
    -- Stack the branches
    SELECT priority AS category, csat AS entity_id
    FROM tickets
    UNION
    SELECT actor, ticket_id
    FROM ticket_events
    WHERE ticket_id >= 50
      AND actor IN ('agent', 'system')
)
SELECT category, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY category
ORDER BY category
