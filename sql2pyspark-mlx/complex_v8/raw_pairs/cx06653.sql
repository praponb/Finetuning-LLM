WITH combined AS (
    -- Stack the branches
    SELECT id AS entity_id, closed_at, agent_id
    FROM tickets
    UNION ALL
    SELECT ticket_id, event_at, id
    FROM ticket_events
)
SELECT entity_id, COUNT(*) AS n_rows, SUM(agent_id) AS total_agent_id, MAX(closed_at) AS latest_closed_at
FROM combined
GROUP BY entity_id
ORDER BY entity_id
