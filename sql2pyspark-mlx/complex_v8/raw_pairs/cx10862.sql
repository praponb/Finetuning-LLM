WITH filtered_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
),
combined AS (
    -- Stack the branches
    SELECT id AS ref_id, ticket_id, 'event' AS source
    FROM filtered_events
    WHERE HOUR(event_at) BETWEEN 7 AND 11
      AND ticket_id IS NOT NULL
    UNION ALL
    SELECT id, ticket_id, 'event_2' AS source
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND HOUR(event_at) BETWEEN 8 AND 11
)
SELECT source, COUNT(*) AS n_rows, SUM(ticket_id) AS total_ticket_id
FROM combined
GROUP BY source
ORDER BY source
