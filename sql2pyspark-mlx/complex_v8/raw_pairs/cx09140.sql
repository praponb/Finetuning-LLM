WITH combined AS (
    -- Stack the branches
    SELECT event_type AS label, actor AS name, id AS entity_id
    FROM ticket_events
    WHERE event_type <> 'escalated'
      AND actor IS NOT NULL
    UNION ALL
    SELECT priority, channel, csat
    FROM tickets
    UNION ALL
    SELECT priority, channel, csat
    FROM tickets
    WHERE channel <> 'email'
      AND priority = 'P3'
)
SELECT label, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY label
ORDER BY label
