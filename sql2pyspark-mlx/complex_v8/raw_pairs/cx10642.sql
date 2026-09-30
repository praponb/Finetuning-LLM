WITH stacked AS (
    -- Stack the branches
    SELECT id, team AS category
    FROM support_agents
    UNION
    SELECT id, actor AS category
    FROM ticket_events
    WHERE event_type NOT IN ('created', 'resolved')
      AND TO_DATE(event_at) < DATE '2025-02-24'
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY category
ORDER BY category
