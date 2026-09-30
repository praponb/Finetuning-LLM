WITH stacked AS (
    -- Stack the branches
    SELECT id, ticket_id, 'event' AS source
    FROM ticket_events
    WHERE event_type <> 'reopened'
    UNION ALL
    SELECT id, ticket_id, 'event_2' AS source
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
      AND actor = 'agent'
)
SELECT source, COUNT(*) AS n_rows, SUM(ticket_id) AS total_ticket_id
FROM stacked
GROUP BY source
ORDER BY source
